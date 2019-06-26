import datetime
import itertools
import json
import time
import pymysql


from ..exeption import DbCriticalExceptionSLL
from ..exeption import DbSqlQueryExceptionSLL
from ..exeption import DemultiplexorCriticalExceptionSLL
from ..exeption import DemultiplexorExceptionSLL

from ..decorators import unknown_exception_catcher
from ..decorators import sll_exception_catcher
from ..decorators import timer
from ..decorators import deprecated

from ..logger import Color
from ..logger import CLogger

from ..mock.mock import MockingHelper

from ..progress_bar.progress_bar import ProgressBar

from .. import other


class BaseMySQLDB:
    """
    Базовый класс для работы БД в частности MySQL
    """

    MAX_CNT_FAST_DEBUG = 5000  # Макс. кол-во обрабатываемых записей в режиме быстрой отладки
    MAX_CNT_DELETE_ROW = 10000  # Макс. кол-во удаляемых строк за один раз в одном запросе
    MAX_CNT_EXECUTEMANY_DATA = 25000  # Макс. кол-во данных в одном executemany запросе
    MAX_CNT_FETCH_ALL = 100000  # Макс. кол-во строк получаемых из одного запроса fetchall
    MAX_CNT_VIEW_LOG_DATA = 10  # Макс. кол-во строк для отображения данных в логировании запросов

    _db_connection = None
    _timeout_reconnect_db = 10  # Таймаут попыток переподключится к БД 1 sec
    _max_cnt_reconnect = 10  # Максимальное число попыток подключится к БД

    def __init__(self,
                 db_config: dict,
                 auto_commit: bool = True,
                 fast_debug: bool = False,
                 charset: any = None,  # latin-1, utf8, utf8mb4
                 local_infile: int = 0):
        self.db_host = db_config["host"]
        self.db_port = db_config["port"]
        self.db_user = db_config["user"]
        self.db_pass = db_config["pass"]
        self.db_name = db_config["name_db"]
        self.autocommit = auto_commit
        self.fast_debug = fast_debug
        self.charset = charset
        self.local_infile = local_infile

    def __enter__(self):
        return self if self.connect() else None

    def __exit__(self, type_e, value_e, traceback_e):
        self.cleanup()

    def db_info(self) -> str:
        return f"{self.db_host}:{self.db_port}/{self.db_name}"

    @unknown_exception_catcher
    @sll_exception_catcher
    def connect(self) -> bool:
        """
        Метод для устанввления соединения с БД
        :return:
            * True - successful
            * False - fail
        """
        cnt = 0
        inst_save = None
        while cnt < 100:
            try:
                cnt += 1
                self._db_connection = pymysql.connect(host=self.db_host,
                                                      port=int(self.db_port),
                                                      user=self.db_user,
                                                      passwd=self.db_pass,
                                                      db=self.db_name,
                                                      autocommit=self.autocommit,
                                                      local_infile=self.local_infile)
                break
            except Exception as inst:
                inst_save = inst
                CLogger.exception(inst,
                                  f"[CONNECT] Can't connect to DB: {self.db_info()}.\n"
                                  f"Attempt {cnt}/100. Timeout: {self._timeout_reconnect_db} sec")
                time.sleep(self._timeout_reconnect_db)
        if cnt == self._max_cnt_reconnect:  # если не получилось подключиться пишем письмо об ошибке
            raise DbCriticalExceptionSLL(message="Can't connect to database :( ( Attemp == 100 (100*1 sec) ). Exit. ",
                                         inst=inst_save)

        CLogger.info(f"[CONNECT] Connect to DB: {Color.Blue}{self.db_info()}. {Color.Light_Green}OPENED!")
        if self.charset:
            CLogger.info(f"[CONNECT] Try change charset to: {self.charset}")
            return self.set_charset(self.charset)
        return True

    def cleanup(self) -> None:
        """
        Закрыть соединение с БД
        :return: None
        """
        CLogger.info(f"[CLEANUP] Connect to DB: {Color.Blue}{self.db_info()}. {Color.Light_Red}CLOSED!")
        self._db_connection.close()

    @unknown_exception_catcher
    def set_charset(self, charset: any = None) -> bool:
        """
        Настройка типа char_set коннекшена
        :param charset: тип настраиваемого коннекшена к БД
        :return:
            * True  - successful
            * False - fail
        """
        chs = charset if charset else self.charset
        self._db_connection.set_charset(chs)
        with self._db_connection.cursor() as cursor:
            return self._execute(cursor, sql=f"SET NAMES {chs};") and \
                   self._execute(cursor, sql=f"SET CHARACTER SET {chs};") and \
                   self._execute(cursor, sql=f"SET character_set_connection={chs};")

    @unknown_exception_catcher
    def execute(self, sql: str, execute_many_data: any = None, get_lastrowid: bool = False, silence: bool = False,
                mocking: bool = False) -> (bool, any):
        """
        Метод для осуществления SQL запроса, без возвращения данных из БД
        Служит для централизованного логирования всех запросов к БД и предоставляет возможности mocking-а запросов
        Внутри есть встроенный "пагинатор" запросов
        :param sql: запрос к БД
        :param execute_many_data: данные для запроса cursor.executemany
        :param get_lastrowid: вернуть id последней вставленной записи (работает только при execute_mny_data=None)
        :param silence: печатаем ли отладочную информацию
        :param mocking: объект для подмены БД
        :return:
            * True, cursor (if flag cursor_return=True) - successful result.
            * None - exceptions exists
        """
        with self._db_connection.cursor() as cursor:
            if execute_many_data:
                offset = 0
                limit = self.MAX_CNT_EXECUTEMANY_DATA \
                    if len(execute_many_data) >= self.MAX_CNT_EXECUTEMANY_DATA else len(execute_many_data)
                while offset < len(execute_many_data):
                    end = offset + limit if len(execute_many_data) >= offset + limit else len(execute_many_data)
                    if self._execute(cursor, sql, execute_many_data[offset:end], silence, mocking) is None:
                        return None
                    if self.fast_debug:
                        break
                    offset = end
                CLogger.info(f"\tSuccessful processed {len(execute_many_data)} records!")
                return True  # successful
            else:
                return self._execute(cursor=cursor, sql=sql, execute_many_data=None,
                                     get_lastrowid=get_lastrowid, silence=silence, mocking=mocking)

    @unknown_exception_catcher
    def execute_and_fetch_all(self, sql: str, sql_template: any = None, callback: any = None, delete: bool = False,
                              silence: bool = False, mocking: bool = False) -> (any, any):
        """
        Метод осуществления запроса к БД с получения ответа.
        Внимание! Если sql_template есть, то берется он, в противном случае берется sql.
        :param sql: готовый sql запрос
        :param sql_template: шаблон запроса к БД (оставлено место для limit/offset)
        верхнее ограничение для кол-ва извлекаемых данных за один запрос
        :param callback: функция колбэк
        :param delete: флаг - используется ли функция для delete запроса (другие лимиты и ненмого логика другая)
        :param silence: печатаем ли отладочную информацию
        :param mocking: объект для подмены БД
        :return:
            * Obj<list>, Cnt<int>  - Result of cursor.fetchall() or Result after callback func
            * None, None           - exceptions exists or errors
        """
        result = []
        offset = 0
        with self._db_connection.cursor() as cursor:
            if sql_template:
                limit = self.MAX_CNT_FETCH_ALL if not delete else self.MAX_CNT_DELETE_ROW
                limit = limit if not self.fast_debug else self.MAX_CNT_FAST_DEBUG
                while True:  # Получаем данные по частям, чтобы избежать переполнение буфера MySQL
                    r, cnt_received = self._execute_and_fetch_all(cursor=cursor,
                                                                  sql=f"{sql_template} LIMIT {offset},{limit};"
                                                                  if not delete else f"{sql_template} LIMIT {limit};",
                                                                  execute_many_data=None,
                                                                  silence=silence,
                                                                  mocking=mocking)
                    if not delete:
                        if r and cnt_received:
                            result.extend(r)
                            offset += cnt_received
                        else:
                            return None
                    else:
                        if cnt_received:
                            CLogger.debug(f"\tSuccessful Delete {cnt_received} records")
                    # Условия выхода (получено меньше чем limit или быстрая отладка)
                    if cursor.rowcount < limit or self.fast_debug:
                        break
            else:
                result, offset = self._execute_and_fetch_all(cursor=cursor,
                                                             sql=sql,
                                                             execute_many_data=None,
                                                             silence=silence,
                                                             mocking=mocking)
        if callback:
            if result is None or offset == 0:
                return None
            try:
                result = callback(result)
            except Exception as inst_callback:
                raise DbCriticalExceptionSLL(f"Problem when call 'callback'! ", inst_callback)
        return result, offset

    @sll_exception_catcher
    @timer
    def _execute(self, cursor: any, sql: str, execute_many_data: any = None, get_lastrowid: bool = False,
                 silence: bool = False, mocking: bool = False) -> any:
        """
        Базовый метод для осуществления SQL запроса.
        Служит для централизованного логирования всех запросов к БД и предоставляет возможности mocking-а запросов
        :param cursor: текущее соединение с БД
        :param sql: запрос к БД
        :param execute_many_data: данные для запроса cursor.executemany
        :param get_lastrowid: получить id последней всталвенной записи
        :param silence: печатаем ли отладочную информацию
        :param mocking: объект для подмены БД
        :return:
            * True - successful result.
            * None - exceptions exists
        """

        cursor = MockingHelper().get_cursor() if mocking else cursor
        if cursor:
            try:
                if not silence:
                    CLogger.printc(f"\tExecute sql query:\n"
                                   f"\t\t{Color.Light_Cyan}{sql}", color=Color.Yellow)
                if execute_many_data:
                    CLogger.printc(f"\tSQL data args:\n"
                                   f"\t\t{Color.Light_Magenta}{execute_many_data[:self.MAX_CNT_VIEW_LOG_DATA]}",
                                   color=Color.Yellow)
                cursor.execute(sql) if execute_many_data is None else cursor.executemany(sql, execute_many_data)
                if not self.autocommit:
                    self._db_connection.commit()  # if autocommit true; doesn't need
                return True if not get_lastrowid else True, cursor.lastrowid  # successful
            except Exception as inst:
                raise DbSqlQueryExceptionSLL(
                    db_info=self.db_info(),
                    sql=sql,
                    sql_args_data=execute_many_data,
                    message=f"Error in func {other.who_am_i()!r}\n",
                    inst=inst)
        else:
            raise DbCriticalExceptionSLL(message=f"Cursor obj is None!", inst=None)

    @sll_exception_catcher
    def _execute_and_fetch_all(self, cursor: any, sql: str, execute_many_data: any = None, silence: bool = False,
                               mocking: bool = False) -> (any, any):
        """
        Базовый метод осуществления запроса к БД с получения ответа.
        :param cursor: текущее соединение с БД
        :param sql: запрос к БД
        :param execute_many_data: данные для запроса cursor.executemany
        :param silence: печатаем ли отладочную информацию
        :param mocking: объект для подмены БД
        :return: result and received rowcount
            * Obj<list>, int  - Result of cursor.fetchall()
            * None, None - exceptions exists or errors
        """
        try:
            if self._execute(cursor, sql, execute_many_data, silence, mocking):
                result = []
                for r in cursor.fetchall():
                    result.append(r)
                return result if len(result) else None, cursor.rowcount
            else:
                return None, None
        except Exception as inst:
            raise DbCriticalExceptionSLL(message=f"Unknown Error!", inst=inst)


class LogDB(BaseMySQLDB):
    """
    Класс для работы с БД Логов модуля (Бони, 365 дневных таблиц)
    """

    def __init__(self,
                 db_config,
                 type_log,
                 date=None,
                 autocommit=True,
                 fast_debug=False,
                 ):
        BaseMySQLDB.__init__(self, db_config=db_config, auto_commit=autocommit, fast_debug=fast_debug)
        self.type_log = type_log  # Тип лога
        self.date = date if date else datetime.datetime.now()  # если не задано берем - текущюю датау
        self.day = self.date.timetuple().tm_yday - 1  # номер таблицы лога - день в году минус 1

    @timer
    def get_count_table(self, where=None):
        """
        Метод для получения кол-ва записей определенного типа в лог таблице
        'SELECT COUNT(1) FROM log_{self.day} WHERE type={self.type_log} AND {where} LIMIT 1;'
        :param where: дополнительно AND условие в запросе
        :return: число записей в таблице
        """
        CLogger.info(f'Try Select COUNT(1) for table log_{self.day}')
        sql = f"SELECT COUNT(1) FROM log_{self.day} WHERE type={self.type_log} AND {where} LIMIT 1;" \
            if where else \
            f"SELECT COUNT(1) FROM log_{self.day} WHERE type={self.type_log} LIMIT 1;"
        r = self.execute_and_fetch_all(sql=sql, callback=lambda x: x[0][0])[0]
        return r if r else None

    @timer
    def get_log_data(self, where=None, order_by=None, group_by_crc=True, ignore_zero_crc=True):
        """
        Базовая функция получения данных из log таблицы
        :param where: дополнительно AND условие в запросе
        :param order_by: сортировка
        :param group_by_crc:
        :param ignore_zero_crc:
        :return:
            * {} or [] (group: TRUE ? FALSE) - successful
            * None - fail
        """

        # В зависимости от выбранной настройки получаем либо:
        #   * [] массив словарей (нет группировки group_by_crc=FALSE)
        #   * {} словарь массивов (есть группировка group_by_crc=TRUE), где ключи - crc пользователей

        def convert_mas_to_dict(arr):
            """
            Функция конвертер массива словарей в словарь массивов
            :param arr: [] массив словарей
            :return: {} словарь массивов (ключи crc - пользователей)
            """
            a = None
            try:
                dd = {}
                for a in arr:
                    if a['crc'] not in dd.keys():
                        dd[a['crc']] = []
                    dd[a['crc']].append(a)
                return dd
            except Exception as inst:
                CLogger.exception(inst, f"Problem in func: {other.who_am_i()!r}.\n Data:{a}")

        def process_log_data(records):
            """
            Предобработка данных логов из БД log
            :param records: [] tuple
            :return: [] of {}
            """
            result = []
            r = None
            try:
                for r in records:
                    d = json.loads(r[3])
                    if "crc" not in d.keys():
                        continue
                    d["time"] = int(datetime.datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S").timestamp())
                    result.append(d)
                return result
            except Exception as inst_parse_log_data:
                raise DbCriticalExceptionSLL(message=f"Error in func {other.who_am_i()!r}. While parse log data {r}",
                                             inst=inst_parse_log_data)

        order_by = f"ORDER BY {order_by}" if order_by else ""
        where = f"{where}{' AND crc!=0' if ignore_zero_crc else ''}" if where else "crc!=0" if ignore_zero_crc else None

        sql_template = f"SELECT * FROM log_{self.day} WHERE type={self.type_log} AND {where} {order_by}" \
            if where else f"SELECT * FROM log_{self.day} WHERE type={self.type_log} {order_by}"
        CLogger.info(f"Estimated cnt record for this select query: {self.get_count_table(where=where)}")
        CLogger.info(f"{Color.Light_Blue}SELECT{CLogger.infoColor} data from {Color.Magenta}log_{self.day} "
                     f"{Color.Blue}{self.db_info()}")
        log_data = self.execute_and_fetch_all(sql=None, sql_template=sql_template, callback=process_log_data)[0]  # ![0]
        return convert_mas_to_dict(log_data) if group_by_crc else log_data if log_data else None

    def get_log_last_x_min(self, x, where=None, order_by=None, group_by_crc=True, ignore_zero_crc=True):
        """
        ВНИМАНИЕ! НЕ БЕЗОПАСНО! В ТОЧКЕ ПЕРЕСЕНЧИЯ ДАТ! НУЖНО БРАТЬ РАЗНЫЕ ТАБЛИЦЫ!
        Получить данные за последние x минут (округление до минут, нижнее)
        :param x: кол-во минут назад (безопасно до 59 включительно)
        :param where: дополнительное условие поиска
        :param order_by: сортировка
        :param group_by_crc: группировать по пользователям
        :param ignore_zero_crc: игнорировать crc==0
        :return: [] or None
        """
        self.day = (self.date - datetime.timedelta(minutes=x)).timetuple().tm_yday - 1
        where = f"{where} AND " if where else ""
        condition = "%s time >= '%s' AND time < '%s'" % \
                    (
                        where,
                        (self.date - datetime.timedelta(minutes=x)).strftime("%Y-%m-%d %H:%M:0"),
                        self.date.strftime("%Y-%m-%d %H:%M:0")
                    )
        return self.get_log_data(condition, order_by, group_by_crc, ignore_zero_crc)

    def get_log_last_x_hour(self, x, where=None, order_by=None, group_by_crc=True, ignore_zero_crc=True):
        """
        Получить данные за чаc x часов назад
        :param x: кол-во часов назад (безопасно до 1 часа включительно)
        :param where: дополнительное условие поиска
        :param order_by: сортировка
        :param group_by_crc: группировать по пользователям
        :param ignore_zero_crc: игнорировать crc==0
        :return: [] or None
        """
        self.day = (self.date - datetime.timedelta(hours=x)).timetuple().tm_yday - 1
        where = f"{where} AND " if where else ""
        condition = "%s time >= '%s' AND time <= '%s'" % \
                    (
                        where,
                        (self.date - datetime.timedelta(hours=x)).strftime("%Y-%m-%d %H:00:00"),
                        (self.date - datetime.timedelta(hours=x)).strftime("%Y-%m-%d %H:59:59")
                    )
        return self.get_log_data(condition, order_by, group_by_crc, ignore_zero_crc)


class StatDB(BaseMySQLDB):
    """
    Класс для работы с БД Аналитиков MySQL
    """

    def __init__(self,
                 db_config,
                 table,
                 date,
                 autocommit=True,
                 fast_debug=False,
                 charset=None,
                 local_infile=0
                 ):
        BaseMySQLDB.__init__(self, db_config=db_config, auto_commit=autocommit, fast_debug=fast_debug, charset=charset,
                             local_infile=local_infile)
        self.date = date
        self.table = table

    @timer
    def get_count_table(self, table=None, where=None):
        """
        Метод для получения кол-ва записей определенного типа в таблице
        'SELECT COUNT(1) FROM {table} {where} LIMIT 1;'
        :param table: имя таблицы
        :param where: where условие в запросе
        :return: число записей в таблице
        """
        table = table if table else self.table
        where = f" WHERE {where}" if where else ""
        sql = f"SELECT COUNT(1) FROM {table} {where} LIMIT 1;"
        CLogger.info(f"{Color.Light_Blue}SELECT COUNT(1){CLogger.infoColor} data from {Color.Magenta}{table} "
                     f"{Color.Blue}{self.db_info()}")
        r = self.execute_and_fetch_all(sql=sql, callback=lambda x: x[0][0])[0]
        return r if r else None

    @timer
    def delete_data(self, order_by, where, offset=None, limit=None, table=None, pure_sql=None):
        """
        Удалить данные из таблицы
         'DELETE FROM {table} {where} {order by} {limit};'
        Внимание! Order by обазательный параметр, игнорировать можно. Нужен чтобы репликация не расходилась.
        :param where: условие удаления
        :param order_by: сортировка при удалении (важна!!!) чтобы не рассыпалась реплика!
        :param offset: смещение
        :param limit: кол-во удаляемых записей
        :param table: таблица откуда удаляем
        :param pure_sql: чистый sql, игнорирует прочие настройки, использовать аккуратно!
        :return:
            * True - successful
            * False - fail
        """
        table = table if table else self.table
        order_by = f" ORDER BY {order_by}" if order_by else ""
        where = f" WHERE {where}" if where else ""
        sql_template = f"DELETE FROM `{table}` {where} {order_by}"

        if pure_sql is None:
            if limit or offset:
                limit = limit if limit else 1
                offset = offset if offset else 0
                pure_sql = f"{sql_template} LIMIT {offset}, {limit};"
                sql_template = None
        else:
            sql_template = None
        CLogger.info(f"{Color.Red}DELETE{CLogger.infoColor} data from {Color.Magenta}{table} "
                     f"{Color.Blue}{self.db_info()}")
        return True if self.execute_and_fetch_all(sql=pure_sql, sql_template=sql_template, delete=True) else False

    @timer
    def insert_multi_data(self, columns, data, table=None):
        """
        Множественный insert в БД c пагинацией
        :param columns: столбцы
        :param data: []<tuple> - данные вставки
        :param table: имя таблицы
        :return:
            * True - success
            * False - fail
        """
        table = table if table else self.table
        if data:
            ss = ('%s,' * len(columns))[:-1]  # '%s,%s,%s,...,%s')
            sql = f"INSERT INTO `{table}` ({','.join(columns)}) VALUES ({ss});"
            CLogger.info(f"{Color.Green}INSERT MULTI{CLogger.infoColor} data to {Color.Magenta}{table} "
                         f"{Color.Blue}{self.db_info()}")
            return self.execute(sql=sql, execute_many_data=data)

    @timer
    def update_multi_data(self, columns, data, condition=None, table=None):
        """
        Множественный update в БД c пагинацией
        :param columns: столбцы
        :param condition: условие обновления
        :param data: []<tuple> - данные обновления
        :param table: имя таблицы
        :return:
            * True - success
            * False - fail
        """
        table = table if table else self.table
        condition = f"WHERE {condition}" if condition else ""
        if data:
            set = "=%s,".join(columns) + "=%s"
            sql = f"UPDATE `{table}` SET {set} {condition};"
            CLogger.info(f"{Color.Yellow}UPDATE MULTI{CLogger.infoColor} data in {Color.Magenta}{table} "
                         f"{Color.Blue}{self.db_info()}")
            return self.execute(sql=sql, execute_many_data=data)

    @sll_exception_catcher
    @timer
    def insert_or_on_duplicate_key_update(self, columns: [], data: [], table: any = None, silence: bool = False):
        """
        Вставка или update при дубликате ключа
        :param columns: столбцы
        :param data: []<tuple> - данные вставки
        :param table: имя таблицы
        :param silence:
        :return:
            * True - success
            * False - fail
        """
        table = table if table else self.table
        if data:
            try:
                with self._db_connection.cursor() as cursor:
                    CLogger.info(f"{Color.Magenta}INSERT on DUPLICATE UPDATE{CLogger.infoColor} data to "
                                 f"{Color.Magenta}{table} {Color.Blue}{self.db_info()}")
                    status_bar = ProgressBar("[INSERT or UPDATE] it is processed", max_cnt_value=len(data),
                                             every_cnt_percent=10, silence=False)
                    for d in data:
                        upd_val = []
                        for i, e in enumerate(d):
                            if e is not None:
                                upd_val.append(columns[i] + "=" + str(e))
                        status_bar.increment_counter()
                        sql = f"INSERT INTO `{table}` ({','.join(columns)}) " \
                            f"VALUES ({','.join(str(e) for e in d)}) " \
                            f"ON DUPLICATE KEY UPDATE {','.join(upd_val)};"
                        self._execute(cursor=cursor, sql=sql, silence=silence)
            except Exception as inst:
                raise DbCriticalExceptionSLL("Unknown error", inst=inst)

    @timer
    def insert_and_get_last_id(self, columns, data, table=None):
        """
        Одиночная вставка и получение id вставленной записи
        :param columns: колонки для инсерта
        :param data: []<str> инсерт данные соотв. колонкам
        :param table: таблица
        :return:
            last_row_id<int> - id вставленной записи  (None if fail)
        """
        last_id = None
        table = table if table else self.table
        if data:
            sql = f"INSERT INTO `{table}` ({','.join(columns)}) VALUES ({','.join(data)});"
            CLogger.info(f"{Color.Green}INSERT{CLogger.infoColor} data to "
                         f"{Color.Magenta}{table} {Color.Blue}{self.db_info()}")
            result, last_id = self.execute(sql=sql, get_lastrowid=True)
        return last_id

    @timer
    def insert_one_record(self, columns: [], data: [], table: any = None) -> any:
        """
        Вставка одиночной записи, просто удобный wrapper
        :param columns: колонки для инсерта
        :param data: []<str> инсерт данные соотв. колонкам
        :param table: таблица
        :return:
            * True - successful result.
            * None - exceptions exists
        """
        table = table if table else self.table
        if data:
            sql = f"INSERT INTO `{table}` ({','.join(columns)}) VALUES ({','.join(data)});"
            CLogger.info(f"{Color.Green}INSERT{CLogger.infoColor} data to "
                         f"{Color.Magenta}{table} {Color.Blue}{self.db_info()}")
            return self.execute(sql=sql)

    @timer
    def select_data(self, columns: [], where: any = None, order_by: any = None, offset: any = None, limit: any = None,
                    table: any = None, pure_sql: any = None, callback: any = None) -> any:
        """
        Внимание! параметр sql используется только для осуществления простых запросов без пагинации.
        :param columns: [] колонок таблицы
        :param where: условие
        :param order_by: условие сортировки
        :param offset: смещение записей
        :param limit: кол-во записей
        :param table: таблица
        :param pure_sql: чистый sql использовать только если понимаешь что делаешь!
        :param callback: функция колбэк для обработки данных
        :return:
            * Obj<list> - Result of cursor.fetchall() or Result after callback func
            * None      - exceptions exists or errors
        """
        where = f" WHERE {where} " if where else ""
        order_by = f" ORDER BY {order_by} " if order_by else ""
        table = table if table else self.table
        sql_template = f"SELECT {','.join(columns)} FROM `{table}` {where} {order_by}"
        if pure_sql is None:
            if limit or offset:
                limit = limit if limit else 1
                offset = offset if offset else 0
                pure_sql = f"{sql_template} LIMIT {offset}, {limit};"
                sql_template = None
        else:
            sql_template = None
        CLogger.info(f"{Color.Blue}SELECT{CLogger.infoColor} data from {Color.Magenta}{table} "
                     f"{Color.Blue}{self.db_info()}")
        r = self.execute_and_fetch_all(sql=pure_sql, sql_template=sql_template, callback=callback)
        return r[0] if r else None


class DemultiplexorDB:
    """
    Класс для удобной работы по схеме (1:M) 1 master - many slave
    """

    def __init__(self, master_db: any, mas_slave_db: any, remove_none_db: bool = True):
        self.master = master_db  # мастер
        self.slaves = [db for db in mas_slave_db if db] if remove_none_db else mas_slave_db  # массив слейвов

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type_e, value_e, traceback_e):
        self.cleanup()

    def connect(self):
        for db in itertools.chain([self.master], self.slaves):
            if db:
                db.connect()

    def cleanup(self):
        for db in itertools.chain([self.master], self.slaves):
            if db:
                db.cleanup()

    @sll_exception_catcher
    def call_master_method(self, method_name: str, *args, **kwargs):
        """
        Функция вызов метода по имени для БД Мастер
        Функция может кидать исключения, но предусмотрительно обернута декоратором для их перехвата
        :param method_name: <str> имя метода
        :param args: <tuple> неименнованные агрументы (кортеж)
        :param kwargs: {} именнованные агрументы (словарь)
        :return: result of method
        """
        try:
            return self.master.__getattribute__(method_name)(*args, **kwargs) if self.master else None
        except AttributeError as inst_ae:
            raise DemultiplexorExceptionSLL(inst=inst_ae, message=f"Master DB hasn't method {method_name} implemented!")
        except TypeError as inst_type_err:
            raise DemultiplexorCriticalExceptionSLL(inst=inst_type_err,
                                                    message=f"Missing or incorrect input param for call {method_name}\n"
                                                    f"Args={args}; Kwargs={kwargs}")
        except Exception as inst_unknown:
            raise DemultiplexorCriticalExceptionSLL(inst=inst_unknown,
                                                    message=f"Unknown error! MasterDB call method by name. ")

    @deprecated
    def call_slaves_method(self, method: str, *args, **kwargs):
        """
        Функция вызов метода по имени для всех Slave БД
        :param method: <str> имя метода
        :param args: <tuple> неименнованные агрументы (кортеж)
        :param kwargs: {} именнованные агрументы (словарь)
        :return: [] result of methods
        """
        result = []
        for i, _ in enumerate(self.slaves):
            result.append(self.call_n_slave_method(i, method, *args, **kwargs))
        return result

    @deprecated
    @sll_exception_catcher
    def call_n_slave_method(self, n: int, method: str, *args, **kwargs):
        """
        Функция вызов метода по имени для одного Slave БД (по его номеру)
        Функция может кидать исключения, но предусмотрительно обернута декоратором для их перехвата
        :param n: <int> номер slave БД
        :param method: <str> имя метода
        :param args: <tuple> неименнованные агрументы (кортеж)
        :param kwargs: {} именнованные агрументы (словарь)
        :return: result of method
        """
        if n > len(self.slaves):
            raise DemultiplexorCriticalExceptionSLL(inst=None,
                                                    message=f"n > len(self.slave) DB! {n} > {len(self.slaves)}")
        try:
            self.slaves[n].__getattribute__(method)(*args, **kwargs) if self.slaves[n] else None
        except AttributeError as inst_ae:
            raise DemultiplexorExceptionSLL(inst=inst_ae,
                                            message=f"Slave DB has not method {method} implemented!")
        except TypeError as inst_type_err:
            raise DemultiplexorCriticalExceptionSLL(inst=inst_type_err,
                                                    message=f"Missing or incorrect input param for call {method}\n"
                                                    f"Args={args}; Kwargs={kwargs}")
        except Exception as inst_unknown:
            raise DemultiplexorCriticalExceptionSLL(inst=inst_unknown, message=f"Unknown error!")

    def slave_execute(self, method: any, *args, **kwargs):
        """
        !Внимание не безопасный метод на разнородных объектах!
        Исполнение передаваемого метода на всех slave (not None)
        :param method: объявление метода, который исполняем  н-р SLL.StatDB.select_data или массив методов
        :param args: <tuple> неименнованные агрументы (кортеж)
        :param kwargs: {} именнованные агрументы (словарь)
        :return: [] result of method
        """
        methods = method if type(method) is list else [method for _ in self.slaves]
        result = []
        for i, _ in enumerate(self.slaves):
            result.append(self.slave_n_execute(i, methods[i], *args, **kwargs))
        return result

    @sll_exception_catcher
    def slave_n_execute(self, n: int, method: any, *args, **kwargs):
        """
        !Внимание не безопасный метод на разнородных объектах!
        Исполнение передаваемого метода на всех slave (not None)
         Функция может кидать исключения, но предусмотрительно обернута декоратором для их перехвата
        :param n: нмоер слейва для вызова
        :param method: объявление метода, который исполняем  н-р SLL.StatDB.select_data или массив методов
        :param args: <tuple> неименнованные агрументы (кортеж)
        :param kwargs: {} именнованные агрументы (словарь)
        :return: [] result of method
        """
        slave = self.slaves[n] if self.slaves[n] else None
        if slave:
            method_list = [func for func in dir(slave) if callable(getattr(slave, func)) and not func.startswith("__")]
            if method.__name__ not in method_list:
                raise DemultiplexorExceptionSLL(inst=TypeError,
                                                message=f"Slave DB {slave.__name__} hasn't method: {method.__name__}")
            else:
                return method(slave, *args, **kwargs)


class ProfLPDB(StatDB):
    """
    Класс для работы с БД профайлов пользователей LP
    """

    def __init__(self,
                 db_config,
                 table,
                 fast_debug=False,
                 autocommit=True,
                 ):
        StatDB.__init__(self, db_config=db_config, table=table, date=None, autocommit=autocommit, fast_debug=fast_debug)

    def get_xml_for_crc_list(self, crc_list: []) -> dict:
        """
        Получить данные по юзерам которые в списке crc_list
        :param crc_list:
        :return:
        """
        if not self.fast_debug:
            result = self.select_data(columns=["crc", "xml"],
                                      where=f"crc in ({(','.join((str(c) for c in crc_list)))})")
        else:  # быстрая отладка грепаем 100 записей из БД
            result = self.select_data(columns=["crc", "xml"],
                                      where=f"crc in ({(','.join((str(c) for c in crc_list)))})",
                                      limit='100')
        crc_xml = {}
        try:
            for r in result:
                crc_xml[r[0]] = r[1]
        except Exception as inst:
            CLogger.exception(inst, "Error while reformat to dict")

        return crc_xml

    def execute_callback_func_on_all_crc(self, callback: any) -> None:
        """
        Получить данные по всем юзерам в БД
        :param callback: функция колбэк
        :return: None
        """
        if not self.fast_debug:
            self.select_data(columns=["crc", "xml"],
                             order_by='crc',
                             callback=callback)
        else:  # быстрая отладка грепаем 100 записей из БД
            self.select_data(columns=["crc", "xml"],
                             order_by='crc',
                             callback=callback,
                             limit='100')