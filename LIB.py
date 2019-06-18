
import json

import re
import time


import redis
import psycopg2
import pymysql
import requests


from decorators import sll_exception_catcher
from decorators import unknown_exception_catcher




from logger import CLogger




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
    _max_cnt_reconnect = 10     # Максимальное число попыток подключится к БД

    def __init__(self,
                 db_config,
                 auto_commit=True,
                 fast_debug=False,
                 charset=None,  # latin-1, utf8, utf8mb4
                 local_infile=0):
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

    def __exit__(self, type, value, traceback):
        self.cleanup()

    def db_info(self):
        return f"{self.db_host}:{self.db_port}/{self.db_name}"

    @unknown_exception_catcher
    @sll_exception_catcher
    def connect(self):
        """
        Метод для устанввления соединения с БД
        :return:
            * True - successful
            * False - fail
        """
        cnt = 0
        inst_save = None
        while cnt < self._max_cnt_reconnect:
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
                                  f"Attempt {cnt}/{self._max_cnt_reconnect}. Timeout: {self._timeout_reconnect_db} sec")
                time.sleep(self._timeout_reconnect_db)
        if cnt == self._max_cnt_reconnect:  # если не получилось подключиться пишем письмо об ошибке
            raise DbCriticalExceptionSLL(message=f"Can't connect to database :( ( Attemp == {self._max_cnt_reconnect}. ({self._timeout_reconnect_db} sec) ). Exit. ",
                                         inst=inst_save)

        CLogger.info(f"[CONNECT] Connect to DB: {Color.Blue}{self.db_info()}. {Color.Light_Green}OPENED!")
        if self.charset:
            CLogger.info(f"[CONNECT] Try change charset to: {self.charset}")
            return self.set_charset(self.charset)
        return True

    def cleanup(self):
        """
        Закрыть соединение с БД
        :return: None
        """
        CLogger.info(f"[CLEANUP] Connect to DB: {Color.Blue}{self.db_info()}. {Color.Light_Red}CLOSED!")
        self._db_connection.close()

    @unknown_exception_catcher
    def set_charset(self, charset=None):
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
    def execute(self, sql, execute_many_data=None, get_lastrowid=False, silence=False, mocking=False):
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
    def execute_and_fetch_all(self, sql, sql_template=None, callback=None, delete=False, silence=False, mocking=False):
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
    def _execute(self, cursor, sql, execute_many_data=None, get_lastrowid=False, silence=False, mocking=False):
        """
        Базовая метод для осуществления SQL запроса.
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
                    message=f"Error in func {who_am_i()!r}\n",
                    inst=inst)
        else:
            raise DbCriticalExceptionSLL(message=f"Cursor obj is None!", inst=None)

    @sll_exception_catcher
    def _execute_and_fetch_all(self, cursor, sql, execute_many_data=None, silence=False, mocking=False):
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


# Модуль поддержки json с комментариями
# Загрузка из файла
# 1. Игнорируем строки:
#     а) в начале которых есть   ignore_match     r'^[ ,    \n]*//+'
#     б) в которых есть          ignore_search     r'---+'
# 2. Вырезаем из строк которые не попали в п.1 все после
#                                split_zero_after  "//"
def load_comment_json(file, ignore_match=r'^[ ,    \n]*//+', ignore_search=r'---+', split_zero_after="//"):
    """
    Функция для поддержки json с комментариями, убирает закормментирвоанные символы
    :param file: исходный json файл
    :param ignore_match: паттерн игнорирования символов в начале строки
    :param ignore_search: паттерн игнорирования символов в строках которых есть этот паттерн
    :param split_zero_after: вырезаением строк которые не попопали под указанные выше паттерны
    :return: json.obj with clean json file
    """
    clean_json = ''.join(line.split(split_zero_after)[0] for line in file if
                         not (re.match(ignore_match, line) or re.search(ignore_search, line)))
    return json.loads(clean_json)


class Redis:
    """
    Класс для работы с redis
    """

    def __init__(self, config: dict, attempts: int=10):
        self.config = config
        self.redis_connect_attempts = attempts
        self.redis_connected = False
        self.redis_client = None

    def __enter__(self):
        return self if self.connect() else None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def redis_info(self)->str:
        return f"{self.config['Host']}:{self.config['Port']} db:{self.config['DB']}"

    def connect(self):
        if not self.redis_connected:
            self.redis_client = redis.StrictRedis(host=self.config["Host"],
                                                  port=self.config["Port"],
                                                  db=self.config["DB"],
                                                  password=self.config["Password"],
                                                  decode_responses=True)
            for attempt in range(self.redis_connect_attempts):
                try:
                    self.redis_connected = self.redis_client.ping()
                    CLogger.info(f"[CONNECT] Connect to REDIS: {Color.Blue}{self.redis_info()}."
                                 f" {Color.Light_Green}OPENED!")
                    return True
                except Exception as inst:
                    CLogger.exception(exception=inst,
                                      text=f"Can't connect to redis {self.redis_info()}. Ping() fail!")
                    time.sleep(0.1)
            CLogger.error(f"Can't connect to redis server! {self.redis_info()}")
            return False

    def cleanup(self):
        CLogger.info(f"[CLEANUP] Connect to REDIS: {Color.Blue}{self.redis_info()}. {Color.Light_Red}CLOSED!")
        if self.redis_connected:
            self.redis_client.connection_pool.disconnect()
            self.redis_connected = False

    def reconnect(self):
        self.cleanup()
        self.connect()




