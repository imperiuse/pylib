import datetime
import functools
import inspect
import json
import logging
import os
import random
import re
import sys
import time
import traceback
from urllib.error import URLError
from urllib.request import Request, urlopen
import redis
import psycopg2
import pymysql
import requests
import smtplib
import ssl

# ОПИСАНИЕ
# Базовая вспомогательная "библиотека" для написания скриптов
#
# Содержит:
# * Декораторы для отладки функций, декораторы для работы с исключениями
# * Классы для работы с БД MYSQL и дочерние классы для работы с конкретными БД логов и статистики
# * Собственный класс исключений
# Содержит реализацию:
#   Логгера
#   EmailSender
#   Профилировщика времени
#   Цветовые схемы
#   Локер (pid file)
#   JSON Unmarshaller with comments mar
#   Telegram msg notify helper
#   HTTP requests helper


# Функция возращает имя вызывающей ее функции
def who_am_i(n=1, m=3):
    return inspect.stack()[n][m]


class BaseExceptionSLL(Exception):
    def __init__(self, message, inst):
        # Call the base class constructor with the parameters it needs
        stack = ''.join(traceback.format_stack()[:-2])  # -2 убираем в списке вызов этой команды и вызов конструктора
        super(BaseExceptionSLL, self).__init__(f"{message}\nStack trace:\n{stack}\nBase inst:\n\t{inst}")

        # Now for your custom code...
        self.inst = inst


# This exception catch by @sll_exception_catcher and exec os.exit(). Because it's CRITICAL!
class LockerCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message, inst):
        super(LockerCriticalExceptionSLL, self).__init__(f"Locker!\n{message}", inst)


# This exception catch by @sll_exception_catcher and exec os.exit(). Because it's CRITICAL!
class ScriptCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message, inst):
        super(ScriptCriticalExceptionSLL, self).__init__(f"Script!\n{message}", inst)


# This exception catch by @sll_exception_catcher and exec os.exit(). Because it's CRITICAL!
class DbCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message, inst):
        super(DbCriticalExceptionSLL, self).__init__(f"DB!\n{message}", inst)


# This exception catch by @sll_exception_catcher and exec os.exit(). Because it's CRITICAL!
class DemultiplexorCriticalExceptionSLL (BaseExceptionSLL):
    def __init__(self, message, inst):
        super(DemultiplexorCriticalExceptionSLL, self).__init__(f"Demultiplexor!\n{message}", inst)


class DemultiplexorExceptionSLL (BaseExceptionSLL):
    def __init__(self, message, inst):
        super(DemultiplexorExceptionSLL, self).__init__(f"Demultiplexor!\n{message}", inst)


class DbSqlQueryExceptionSLL(BaseExceptionSLL):
    def __init__(self, db_info, sql, sql_args_data, message, inst):
        message = f"Problem with SQL query.\n" \
            f"Description:\n" \
            f"\t{message}\n" \
            f"DB info:\n" \
            f"\t{db_info}\n" \
            f"SQL:\n" \
            f"\t{sql}\n" \
            f"SQL Args:\n" \
            f"\t{sql_args_data}"
        super(DbSqlQueryExceptionSLL, self).__init__(message, inst)


class EmailSenderExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any,
                 email: any, dest_email: any, smtp_server: any, port: any, email_message: any):
        super(EmailSenderExceptionSLL, self).__init__(f"EmailSender!\n{message}", inst)
        self.info = f"\nSMPT: {smtp_server}:{port}\nFrom: {email}, To: {dest_email}\n{email_message}"


# Удобные функции декораторы для отладки
def debug(func):
    """Print the function signature and return value"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        CLogger.debug(f"Calling {func.__name__}({signature})")
        value = func(*args, **kwargs)
        CLogger.debug(f"{func.__name__!r} returned {value!r}")
        return value

    return wrapper


def timer(func):
    """Print the runtime of the decorated function"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        CLogger.debug(f"{Color.Light_Gray}\t<-- Profile {func.__name__!r}. Run time: {run_time:.5f} secs -->")
        return value

    return wrapper


def deprecated(func):
    """Print that the func is deprecated"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if CLogger.emailColor:
            CLogger.warning(f"{Color.Bold}{Color.Light_Red}\t !!! DEPRECATED function {func.__name__!r} !!!")
        else:
            CLogger.warning(f"!!! DEPRECATED function {func.__name__!r} !!!")
        value = func(*args, **kwargs)
        return value

    return wrapper


def nothing_to_do(func):
    """Nothing to do"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return

    return wrapper


# Функции декораторы для удобной обработки функций выбрасывающих исключения
# Примечание на будущее finally блок очень плохо работает в данном случае,
# по сути нивелирует все то ради чего делался декоратор
def unknown_exception_catcher(func):
    """
    Обработка неизвестных исключений.
    ВНИМАНИЕ! Все известные SLL исключения не гасятся, а пробрасываются наверх.
    :param func: функция исключения которой ловим
    :return: итоговая функция враппер
    """

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        try:
            value = func(*args, **kwargs)
            return value
        except BaseExceptionSLL as inst_base:
            raise inst_base
        except Exception as inst:
            CLogger.exception(inst, f"Unknown exception in func {func.__name__}")

    return wrapper_decorator


def sll_exception_catcher(func):
    """
    Обработка исключений SLL.
    ВНИМАНИЕ! Неизвестные исключения пробрасываются наверх!
    :param func: функция исключения которой ловим
    :return: итоговая функция враппер
    """

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        try:
            value = func(*args, **kwargs)
            return value
        except LockerCriticalExceptionSLL as inst_locker_critical:
            CLogger.critical_exception(exception=inst_locker_critical,
                                       text=f"\n[CRITICAL SLL EXCEPTION] by Locker in method {func.__name__}",
                                       err_code=1)
        except DbCriticalExceptionSLL as inst_db_critical:
            CLogger.critical_exception(exception=inst_db_critical,
                                       text=f"\n[CRITICAL SLL EXCEPTION] by DB in method {func.__name__}",
                                       err_code=2)
        except ScriptCriticalExceptionSLL as inst_script_critical:
            CLogger.critical_exception(exception=inst_script_critical,
                                       text=f"\n[CRITICAL SLL EXCEPTION] by SCRIPT in method {func.__name__}",
                                       err_code=3)
        except DemultiplexorCriticalExceptionSLL as inst_demultiplexor_critical:
            CLogger.critical_exception(exception=inst_demultiplexor_critical,
                                       text=f"\n[CRITICAL SLL EXCEPTION] by Demultiplexor in method {func.__name__}",
                                       err_code=4)
        except DemultiplexorExceptionSLL as inst_demultiplexor:
            CLogger.exception(exception=inst_demultiplexor,
                              text=f"\n[SLL EXCEPTION] by Demultiplexor in method {func.__name__}")
        except DbSqlQueryExceptionSLL as inst_db_sql_query:
            CLogger.exception(inst_db_sql_query, f"\nDB SQL QUERY [SLL Exception] in method {func.__name__}")
        except EmailSenderExceptionSLL as inst_email_sender:
            CLogger.Logger.error(f"\nEmail Send [SLL Exception] in method {func.__name__}" +
                                 f"Addition info:{inst_email_sender.info}"
                                 f"Exeption: {inst_email_sender}"
                                 )
        except BaseExceptionSLL as inst_base_sll:
            CLogger.exception(inst_base_sll, f"\nBase [SLL Exception] in func {func.__name__}")
        except Exception as unknown:
            raise unknown

    return wrapper_decorator


# Класс Цвета ANSII
class Color:

    # Escape sequence	Text attributes
    Off = "\x1b[0m"  # All attributes off(color at startup)
    Bold = "\x1b[1m"  # Bold on(enable foreground intensity)
    Underline = "\x1b[4m"  # Underline on
    Blink = "\x1b[5m"  # Blink on(enable background intensity)
    Bold_off = "\x1b[21m"  # Bold off(disable foreground intensity)
    Underline_off = "\x1b[24m"  # Underline off
    Blink_off = "\x1b[25m"  # Blink off(disable background intensity)

    Black = "\x1b[30m"  # Black
    Red = "\x1b[31m"  # Red
    Green = "\x1b[32m"  # Green
    Yellow = "\x1b[33m"  # Yellow
    Blue = "\x1b[34m"  # Blue
    Magenta = "\x1b[35m"  # Magenta
    Cyan = "\x1b[36m"  # Cyan
    White = "\x1b[37m"  # White
    Default = "\x1b[39m"  # Default(foreground color at startup)
    Light_Gray = "\x1b[90m"  # Light Gray
    Light_Red = "\x1b[91m"  # Light Red
    Light_Green = "\x1b[92m"  # Light Green
    Light_Yellow = "\x1b[93m"  # Light Yellow
    Light_Blue = "\x1b[94m"  # Light Blue
    Light_Magenta = "\x1b[95m"  # Light Magenta
    Light_Cyan = "\x1b[96m"  # Light Cyan
    Light_White = "\x1b[97m"  # Light White
    Reset = "\x1b[0m"

    # "\x1b[40m"   # Black
    # "\x1b[41m"   # Red
    # "\x1b[42m"   # Green
    # "\x1b[43m"   # Yellow
    # "\x1b[44m"   # Blue
    # "\x1b[45m"   # Magenta
    # "\x1b[46m"   # Cyan
    # "\x1b[47m"   # White
    # "\x1b[49m"   # Default(background color at startup)
    # "\x1b[100m"  # Light Gray
    # "\x1b[101m"  # Light Red
    # "\x1b[102m"  # Light Green
    # "\x1b[103m"  # Light Yellow
    # "\x1b[104m"  # Light Blue
    # "\x1b[105m"  # Light Magenta
    # "\x1b[106m"  # Light Cyan
    # "\x1b[107m"  # Light White


COLOR = Color()


# Filter for Logger
class LessThanFilter(logging.Filter):

    __slots__ = ('max_level',)

    def __init__(self, exclusive_maximum, name=""):
        super(LessThanFilter, self).__init__(name)
        self.max_level = exclusive_maximum

    def filter(self, record):
        # non-zero return means we log this message
        return 1 if record.levelno < self.max_level else 0


@sll_exception_catcher
def email_send(smtp_server, port, email, dest_email, password, subject, message):
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(email, password)
            server.sendmail(email, dest_email,
                            f"From: {email}\nTo: {dest_email}\nSubject: {subject}\n"
                            f"\n{message.encode('utf-8', errors='ignore')}")
    except Exception as inst:
        raise EmailSenderExceptionSLL(message=f"Something went wrong while send email to {dest_email}", inst=inst,
                                      email=email, dest_email=dest_email, email_message=message,
                                      smtp_server=smtp_server, port=port)


# Основная идея. Все что не ошибка фигачим в STDOUT, все ошибки в STDERR
# Get the root logger
Logger = logging.getLogger()

# Have to set the root logger level, it defaults to logging.WARNING
Logger.setLevel(logging.NOTSET)

logging_handler_out = logging.StreamHandler(sys.stdout)
logging_handler_out.setLevel(logging.DEBUG)
logging_handler_out.addFilter(LessThanFilter(logging.WARNING))
Logger.addHandler(logging_handler_out)

logging_handler_err = logging.StreamHandler(sys.stderr)
logging_handler_err.setLevel(logging.WARNING)
Logger.addHandler(logging_handler_err)


class ColorLogger:
    """
    Цветной логгер и с широким спектром возможностей.
    """
    Reset = "\x1b[0m"
    infoColor = COLOR.Green
    debugColor = COLOR.Yellow
    printColor = COLOR.Blue
    warningColor = COLOR.Yellow
    exceptionColor = COLOR.Red
    errorColor = COLOR.Red
    criticalColor = COLOR.Red
    debug_info_on = True
    emailColor = False
    terminate_critical_exception = True
    custom_email_config = None

    # Example
    # custom_email_config = {
    #       "smtp_server":"smtp.yandex.ru",
    #       "port" : 465,
    #       "email" : "from@ya.ru",
    #       "dest_email" : "to@ya.ru",
    #       "password" : "password",
    #       "subject" : "Analytics Scripts"}

    def __init__(self, logger,
                 info_color=COLOR.Green,
                 print_color=COLOR.Blue,
                 debug_color=COLOR.Yellow,
                 error_color=COLOR.Red):
        self.Logger = logger
        self.infoColor = info_color
        self.debugColor = debug_color
        self.printColor = print_color
        self.warningColor = debug_color
        self.exceptionColor = error_color
        self.errorColor = error_color
        self.criticalColor = error_color
        self.debug_info_on = True

    def config_logger(self, email_color=False, debug_info_on=True, info_color="\x1b[32m", debug_color="\x1b[33m",
                      error_color="\x1b[31m", terminate_critical_exception="True", custom_email_config=None):
        self.infoColor = info_color
        self.debugColor = debug_color
        self.errorColor = error_color
        self.debug_info_on = debug_info_on
        self.emailColor = email_color
        self.terminate_critical_exception = terminate_critical_exception
        self.custom_email_config = custom_email_config

    def custom_email_notify(self, message: str):
        if self.custom_email_config:
            email_send(**self.custom_email_config, message=message)

    def info(self, text: str):
        self.Logger.info(f"{self.infoColor}{text}{self.Reset}")

    def print(self, text: str):
        self.Logger.info(f"{self.printColor}{text}{self.Reset}")

    def printc(self, text: str, color: str):
        self.Logger.info(f"{color}{text}{self.Reset}")

    def debug(self, text: str):
        if self.debug_info_on:
            self.Logger.debug(f"{self.debugColor}{text}{self.Reset}")

    def warning(self, text: str):
        self.custom_email_notify(text)
        self.Logger.warning(f"{self.warningColor}{text}{self.Reset}" if self.emailColor else f"{text}")

    def error(self, text: str):
        self.custom_email_notify(text)
        self.Logger.error(f"{self.errorColor}{text}{self.Reset}" if self.emailColor else f"{text}")

    def critical_error(self, errcode: int, text: str):
        self.error(text)
        if self.terminate_critical_exception:
            exit(errcode)

    def exception(self, exception: any, text: str):
        self.custom_email_notify(f"{text}\n{exception}\n")
        self.Logger.error(f"{self.exceptionColor}{text}\n{exception}\n{self.Reset}"
                          if self.emailColor else f"{text}\n{exception}\n")

    def critical_exception(self, exception: any, text: str, err_code: int):
        self.exception(exception, text)
        if self.terminate_critical_exception:
            exit(err_code)


CLogger = ColorLogger(Logger)


class Locker:
    """
    Локер для недопущения повторного запуска скрипта
        * 1. Проверить есть ли файл.
        * 1.1 Прочитать его (получить PID), проверить есть ли процесс с этим PID.
        * 1.2 Если есть - значит один экземпляр скрипта запущен, и мы завершаемся.
        * 2. Если нет - создаем файл,
        * 2.1 пишем в него PID Текущего процесса
        * 1.2 выполняем скрипт.
        * 3. Удаляем файл.  *
    """

    __slots__ = ('_path_lock_file', '_name_script')

    @property
    def name_script(self):
        return self._name_script

    @property
    def path_lock_file(self):
        return self._path_lock_file

    def __init__(self, path_lock_file: str, name_script=""):
        self._path_lock_file = path_lock_file
        self._name_script = name_script

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, type, value, traceback):
        self.unlock()

    @sll_exception_catcher
    def lock(self, timedelta_before_unlock=datetime.timedelta(hours=1)):
        CLogger.printc(f"Check exist lock file in: {self.path_lock_file}", Color.White)
        try:
            with open(self.path_lock_file, 'r', encoding='utf-8') as lock_file:
                # info = lock_file.read().split(' ')
                # print(info[2], info[3])
                # print(os.system("ps -ax | grep %s"%(info[3])))
                json_data = json.load(lock_file)
                date_lock_file_create = datetime.datetime.strptime(json_data["Date"], "%Y-%m-%d %H:%M:%S")
                if datetime.datetime.now() - date_lock_file_create < timedelta_before_unlock:
                    CLogger.error("Another script already work!")
                    CLogger.error(f"Lock file created at {date_lock_file_create}, less than {timedelta_before_unlock}!")
                    raise LockerCriticalExceptionSLL(f"Another script already work! Exit(1)\n"
                                                     f"Lock file created at {date_lock_file_create},\
                                              less than {timedelta_before_unlock}!", None)
            self.unlock()
            raise FileNotFoundError
        except FileNotFoundError:
            CLogger.printc(f"Lock file not found. Try create lock file: {self.path_lock_file}", Color.White)
            try:
                with open(self.path_lock_file, 'w', encoding='utf-8') as lock_file:
                    json.dump({
                        "Script_name": self.name_script,
                        "Pid": os.getpid(),
                        "Date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, lock_file)
            except Exception as inst1:
                raise LockerCriticalExceptionSLL("Error while json.dump()", inst1)

    @sll_exception_catcher
    def unlock(self):
        CLogger.printc(f"Try remove lock file: {self.path_lock_file}", Color.White)
        try:
            os.remove(self.path_lock_file)
        except FileNotFoundError:
            CLogger.warning(f"Warning! File not found: {self.path_lock_file}")
        except Exception as unknown_inst:
            raise LockerCriticalExceptionSLL("Unknown Error", unknown_inst)


class ProgressBar:
    """
    ProgressBar
    """
    __slots__ = ('_info_str', 'silence', 'max_cnt_value', 'counter', 'percents', 'one_percent', 'every_cnt_percent')

    @property
    def info_str(self):
        return self._info_str

    def __init__(self, info_str, max_cnt_value, every_cnt_percent=1, silence=False):
        try:
            self._info_str = info_str
            self.silence = silence
            self.max_cnt_value = max_cnt_value
            self.counter = 0
            self.percents = 0
            self.one_percent = round(max_cnt_value) / 100 if max_cnt_value > 100 else 1
            self.every_cnt_percent = every_cnt_percent
        except Exception as inst:
            CLogger.exception(inst, "ProcessingCounter __init__ error")

    def increment_counter_n(self, n):
        self.counter = self.counter + n - 1
        self.increment_counter()

    def increment_counter(self):
        self.counter += 1
        self.percents = self.counter // self.one_percent

        if (not self.silence) and (self.counter % (self.one_percent * self.every_cnt_percent) == 0):
            self.__say()

    def __say(self):
        CLogger.info(f"{self.info_str}. Percent:{self.percents}%%. (Counter = {self.counter})")


class Profiler:
    """
    Профайлер времени выполнения участка скрипта
    """
    __slots__ = ('on', 'info_str', 'startTime')

    def __init__(self, on=False, info_str=""):
        self.info_str = info_str
        self.on = on

    def __enter__(self):
        if self.on:
            self.startTime = time.perf_counter()

    def __exit__(self, type, value, traceback):
        if self.on:
            CLogger.info(f"{self.info_str} Time execute block: {time.perf_counter() - self.startTime:.5f} sec")
            # мутное место оставил по старому подстановку переменных


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
    Класс для работы с Redis
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
            CLogger.error(f"Can't connect to Redis server! {self.redis_info()}")
            return False

    def cleanup(self):
        CLogger.info(f"[CLEANUP] Connect to REDIS: {Color.Blue}{self.redis_info()}. {Color.Light_Red}CLOSED!")
        if self.redis_connected:
            self.redis_client.connection_pool.disconnect()
            self.redis_connected = False

    def reconnect(self):
        self.cleanup()
        self.connect()


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
                            message=f"Error in func {who_am_i()!r}\n",
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


"""
########################################################################################################################
###################### H T T P     R E Q E S T S     H E L P E R S    ##################################################
########################################################################################################################

"""

"""
   >>> import requests
   >>> r = requests.get('https://www.python.org')
   >>> r.status_code
   200
   >>> 'Python is a programming language' in r.content
   True

... or POST:

   >>> payload = dict(key1='value1', key2='value2')
   >>> r = requests.post('https://httpbin.org/post', data=payload)
   >>> print(r.text)
   {
     ...
     "form": {
       "key2": "value2",
       "key1": "value1"
     },
     ...
   }
"""


def simple_http_get(url: str, data: dict)->any:
    """
    Simple GET request
    :param url: url
    :param data: dict with http param
    :return: Reponse
    """
    # url = "https://url.com"
    # data = {
    #     'param1': 'value_param_1'
    # }
    return requests.get(url, params=data)


def simple_http_post(url: str, data: dict, json: dict, kwargs)->any:
   """
   Simple POST request
   :param url: url
   :param data: dist with param
   :param json: dict
   :param kwargs: other
   :return:
   """
   return requests.post(url, data, json, **kwargs)


def http_request(url=None, method=None, data=None, headers={}, origin_req_host=None, unverifiable=False,
                 save_html_path=None):
    """
    Функция обертка над стандартной http Request либой Python3, для удобной работы с http запросам
    :param url: полный url для реквеста
    :param method: get/post/head and etc.
    :param data: data for post request
    :param headers: http headers
    :param origin_req_host:
    :param unverifiable:
    :param save_html_path: сохранить полученный response (html файл) на диск в указанное место (save_html_path)
    :return: str  -  html, json, xml  response
    """
    r = Request(
        url=url,
        method=method,
        headers=headers,
        data=data,
        origin_req_host=origin_req_host,
        unverifiable=unverifiable
    )
    try:
        CLogger.debug(f"Try HTTP request: {r.get_full_url()}")
        response = urlopen(r)
    except URLError as e:
        CLogger.debug(e)
        # if hasattr(e, 'reason'):
        #     CLogger.debug('HTTP error request')
        #     CLogger.debug(f"Reason: {e.reason}")
        # elif hasattr(e, 'code'):
        #     CLogger.debug("The server couldn't fulfill the request.")
        #     CLogger.debug(f"Error code: {e.code}")
        return None, e
    else:  # OK
        binary_html = None
        try:
            binary_html = response.read()
            charset = response.info().get_content_charset() if response.info().get_content_charset() else 'UTF-8'
            utf8_html = binary_html.decode(charset)  # UTF-8 f.e.
            if save_html_path:
                try:
                    with open(save_html_path, "w") as f:
                        f.write(utf8_html)
                except Exception as inst2:
                    CLogger.exception(inst2, "Cant save html response. [Func: http_request]")
            return utf8_html, None
        except Exception as inst:
            CLogger.exception(inst, "Read html request error. [Func: http_request]")
            CLogger.error(f"\nDetailed info\nURL:{r.get_full_url()}")
            CLogger.error(f"\nBinary_html:{binary_html}")
            return None, inst


def get_random_user_agent():
    """
    Функция рандомизатор USER AGENTS
    :return: random user agent
    """
    user_agents_list = [
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
        "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1"
    ]

    return user_agents_list[random.randrange(len(user_agents_list))]
