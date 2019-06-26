import smtplib
import ssl
import traceback
from email.mime.text import MIMEText
import logging
import sys
import functools
import time

from .color import Color

COLOR = Color()


class BaseExceptionSLL(Exception):
    def __init__(self, message: str, inst: any):
        # Call the base class constructor with the parameters it needs
        stack = ''.join(traceback.format_stack()[:-2][:3])
        # [:-2] - убираем в списке вызов этой команды и вызов конструктора
        # [:3]  - ограничение глубины стека до 3-х строк
        super(BaseExceptionSLL, self).__init__(f"{message}\nStack trace:\n{stack}\nBase inst:\n\t{inst}")

        # Now for your custom code...
        self.inst = inst


class LockerCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(LockerCriticalExceptionSLL, self).__init__(f"Locker!\n{message}", inst)


class ScriptCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(ScriptCriticalExceptionSLL, self).__init__(f"Script!\n{message}", inst)


class DbCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(DbCriticalExceptionSLL, self).__init__(f"DB!\n{message}", inst)


class DemultiplexorCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(DemultiplexorCriticalExceptionSLL, self).__init__(f"Demultiplexor!\n{message}", inst)


class DemultiplexorExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(DemultiplexorExceptionSLL, self).__init__(f"Demultiplexor!\n{message}", inst)


class DbSqlQueryExceptionSLL(BaseExceptionSLL):
    def __init__(self, db_info: str, sql: str, sql_args_data: any, message: str, inst: any):
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


class EmailSendExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any,
                 email: any, dest_email: any, smtp_server: any, port: any, email_message: any):
        super(EmailSendExceptionSLL, self).__init__(f"EmailSender!\n{message}", inst)
        self.info = f"\nSMPT: {smtp_server}:{port}\nFrom: {email}, To: {dest_email}\n{email_message}"


class SimpleEmailSendExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any, email: any, dest_email: any, email_message: any):
        super(SimpleEmailSendExceptionSLL, self).__init__(f"SimpleEmailSender!\n{message}", inst)
        self.info = f"From: {email}, To: {dest_email}\n{email_message}"


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


def simple_email_send(email, dest_email, subject, message):
    try:
        msg = MIMEText(f'{message}')
        msg['Subject'] = f'{subject}'
        msg['From'] = f'{email}'
        msg['To'] = f'{", ".join(dest_email)}'
        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
    except Exception as inst:
        raise SimpleEmailSendExceptionSLL(message=f"Something went wrong while `simple_email_send` to {dest_email}",
                                          inst=inst, email=email, dest_email=dest_email, email_message=message)


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
        except EmailSendExceptionSLL as inst_email_sender:
            #  CLogger.Logger - не ошибка !! чтобы не было рекурсивного зацикливания
            CLogger.Logger.error(f"\nEmail Send [SLL Exception] in method {func.__name__}" +
                                 f"Addition info:{inst_email_sender.info}"
                                 f"Exception: {inst_email_sender}")
        except SimpleEmailSendExceptionSLL as inst_simple_email_sender:
            #  CLogger.Logger - не ошибка !! чтобы не было рекурсивного зацикливания
            CLogger.Logger.error(f"\nSimple Email Send [SLL Exception] in method {func.__name__}" +
                                 f"Addition info:{inst_simple_email_sender.info}"
                                 f"Exception: {inst_simple_email_sender}")

        except BaseExceptionSLL as inst_base_sll:
            CLogger.exception(inst_base_sll, f"\nBase [SLL Exception] in func {func.__name__}")
        except Exception as unknown:
            raise unknown

    return wrapper_decorator


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
        raise EmailSendExceptionSLL(message=f"Something went wrong while `email_send` to {dest_email}", inst=inst,
                                   email=email, dest_email=dest_email, email_message=message,
                                   smtp_server=smtp_server, port=port)


@sll_exception_catcher
def simple_email_send(email, dest_email, subject, message):
    try:
        msg = MIMEText(f'{message}')
        msg['Subject'] = f'{subject}'
        msg['From'] = f'{email}'
        msg['To'] = f'{", ".join(dest_email)}'
        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
    except Exception as inst:
        raise SimpleEmailSendExceptionSLL(message=f"Something went wrong while `simple_email_send` to {dest_email}",
                                         inst=inst, email=email, dest_email=dest_email, email_message=message)


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



# Filter for logger.py
class LessThanFilter(logging.Filter):

    __slots__ = ('max_level',)

    def __init__(self, exclusive_maximum, name=""):
        super(LessThanFilter, self).__init__(name)
        self.max_level = exclusive_maximum

    def filter(self, record):
        # non-zero return means we log this message
        return 1 if record.levelno < self.max_level else 0


# Основная идея. Все что не ошибка фигачим в STDOUT, все ошибки в STDERR
# Get the root logger.py
Logger = logging.getLogger()

# Have to set the root logger.py level, it defaults to logging.WARNING
Logger.setLevel(logging.NOTSET)

logging_handler_out = logging.StreamHandler(sys.stdout)
logging_handler_out.setLevel(logging.DEBUG)
logging_handler_out.addFilter(LessThanFilter(logging.WARNING))
Logger.addHandler(logging_handler_out)

logging_handler_err = logging.StreamHandler(sys.stderr)
logging_handler_err.setLevel(logging.WARNING)
Logger.addHandler(logging_handler_err)

CLogger = ColorLogger(Logger)
