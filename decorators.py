import functools
import time


from .logger import CLogger
from .logger.color import Color

from .exeption import BaseExceptionSLL
from .exeption import LockerCriticalExceptionSLL
from .exeption import ScriptCriticalExceptionSLL
from .exeption import DbCriticalExceptionSLL
from .exeption import DemultiplexorCriticalExceptionSLL
from .exeption import DemultiplexorExceptionSLL
from .exeption import DbSqlQueryExceptionSLL


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