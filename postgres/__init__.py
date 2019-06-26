import psycopg2
import time

from ..logger import DbCriticalExceptionSLL, DbSqlQueryExceptionSLL, unknown_exception_catcher, sll_exception_catcher, timer
from ..logger import Color, Logger, CLogger
from .. import other


class BasePostgresDB:
    """
    Базовый класс для работы БД в частности POSTGRES
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
        self.db_config = db_config
        self.db_host = db_config["host"]
        self.db_port = db_config["port"]
        self.db_user = db_config["user"]
        self.db_pass = db_config["password"]
        self.db_name = db_config["dbname"]
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
                self._db_connection = psycopg2.connect(**self.db_config)
                self._db_connection.autocommit = self.autocommit
                break
            except Exception as inst:
                inst_save = inst
                CLogger.exception(inst,
                                  f"[CONNECT] Can't connect to DB: {self.db_info()}.\n"
                                  f"Attempt {cnt}/{self._max_cnt_reconnect}. Timeout: {self._timeout_reconnect_db} sec")
                time.sleep(self._timeout_reconnect_db)
        if cnt == self._max_cnt_reconnect:  # если не получилось подключиться пишем письмо об ошибке
            raise DbCriticalExceptionSLL(message=f"Can't connect to database :( ( Attemp == {self._max_cnt_reconnect}). Exit. ",
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
            return self.execute(cursor, sql=f"SET NAMES {chs};") and \
                   self.execute(cursor, sql=f"SET CHARACTER SET {chs};") and \
                   self.execute(cursor, sql=f"SET character_set_connection={chs};")

    @unknown_exception_catcher
    @sll_exception_catcher
    @timer
    def execute_query(self, sql: str)-> []:
        """
        Выполнить запрос с возращением данных
        :param sql: sql запрос
        :return: None
        """
        result = []
        try:
            with self._db_connection.cursor() as cursor:
                if cursor:
                    CLogger.printc(f"\tExecute sql query:\n"
                                   f"\t\t{Color.Light_Cyan}{sql}", color=Color.Yellow)
                    try:
                        cursor.execute(sql)
                        for r in cursor.fetchall():
                            result.append(r)
                        return result
                    except Exception as inst:
                        raise DbSqlQueryExceptionSLL(
                            db_info=self.db_info(),
                            sql=sql,
                            sql_args_data=None,
                            message=f"Error in func {other.who_am_i()!r}\n",
                            inst=inst)
                else:
                    CLogger.info("Try reconnect to DB")
                    self.connect()

        except Exception as inst:
            raise DbCriticalExceptionSLL(message=f"Unknown Error!", inst=inst)

    @unknown_exception_catcher
    @sll_exception_catcher
    @timer
    def execute(self, sql: str)->None:
        """
        Выполнение SQL запроса без возращения данных
        :param sql: sql запрос
        :return: None
        """
        try:
            with self._db_connection.cursor() as cursor:
                if cursor:
                    CLogger.printc(f"\tExecute sql query:\n"
                                   f"\t\t{Color.Light_Cyan}{sql}", color=Color.Yellow)
                    try:
                        cursor.execute(sql)
                    except Exception as inst:
                        Logger.error("{c} Error while execute() work with cursor."
                                     "Exception:{exc}. DB_NAME:{name_db} SQL:{SQL} {r}"
                                     .format(exc=inst, name_db=self.db_config["database"], SQL=sql, c=Color.Cyan,
                                             r=Color.Reset))
                else:
                    CLogger.info("Try reconnect to DB")
                    self.connect()
        except Exception as inst:
            raise DbCriticalExceptionSLL(message=f"Unknown Error!", inst=inst)
