import datetime
import json
import os

from ..logger.logger import sll_exception_catcher, LockerCriticalExceptionSLL, CLogger
from ..logger.color import Color


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

    def __init__(self, path_lock_file: str, name_script: str = ""):
        self._path_lock_file = path_lock_file
        self._name_script = name_script

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, type_e, value_e, traceback_e):
        self.unlock()

    @sll_exception_catcher
    def lock(self, timedelta_before_unlock: datetime.timedelta = datetime.timedelta(hours=1)) -> None:
        """
        Создать лок файл.
        Если он есть, проверить время создание, если оно меньше timedelta_before_unlock, то кинуть Critical Exception!
        В противном случае, удалить лок файл.
        :param timedelta_before_unlock:
        :return: None
        """
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
                else:
                    self.unlock()
                    raise FileNotFoundError
        except FileNotFoundError as inst:
            CLogger.printc(f"Lock file not found. Try create lock file: {self.path_lock_file}", Color.White)
            try:
                with open(self.path_lock_file, 'w', encoding='utf-8') as lock_file:
                    json.dump({
                        "Script_name": self.name_script,
                        "Pid": os.getpid(),
                        "Date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
                        lock_file)
            except Exception as inst1:
                raise LockerCriticalExceptionSLL("Error while json.dump()", inst1)
        except Exception as unknown_inst:
            raise LockerCriticalExceptionSLL(f"Unknown exception", unknown_inst)

    @sll_exception_catcher
    def unlock(self) -> None:
        """
        Удалить лок файл, если он есть.
        Кинуть варнинг, если его нет.
        :return: None
        """
        CLogger.printc(f"Try remove lock file: {self.path_lock_file}", Color.White)
        try:
            os.remove(self.path_lock_file)
        except FileNotFoundError:
            CLogger.warning(f"Warning! Lock file not found: {self.path_lock_file}")
        except Exception as unknown_inst:
            raise LockerCriticalExceptionSLL("Unknown Error", unknown_inst)
