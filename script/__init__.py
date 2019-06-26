import datetime

from ..logger import CLogger, Color, ScriptCriticalExceptionSLL, sll_exception_catcher
from ..locker import Locker
from ..profiler import Profiler
from ..cfg import CFG


class Script:
    """
    Базовый класс для скриптов
    """

    __slots__ = ('_script_name', '_start_time', '_options', '_namespace', '_locker', '_ttl_lock_file', 'cfg',
                 'cfg_host', 'DATE', 'DATE_of_DATE', 'DAY', 'MONTH', 'DATE_FROM', 'DATE_TO', 'DATE_TIME_FROM',
                 'cfg_host', 'DATE', 'DATE_of_DATE', 'DAY', 'MONTH', 'DATE_FROM', 'DATE_TO', 'DATE_TIME_FROM',
                 'DATE_TIME_TO', '_create_lock_file')

    @property
    def script_name(self):
        return self._script_name

    @property
    def start_time(self):
        return self._start_time

    @property
    def path_lock_file(self):
        return f"/tmp/{self.script_name}.lock"

    @property
    def locker(self):
        return self._locker

    @property
    def namespace(self):
        return self._namespace

    @property
    def options(self):
        return self._options

    @property
    def ttl_lock_file(self):
        return self._ttl_lock_file

    @property
    def lock_file_status(self):
        return self._create_lock_file

    @sll_exception_catcher
    def __init__(self, name: str, namespace, default_date=datetime.datetime.now() - datetime.timedelta(days=1),
                 create_lock_file: bool = True, **options: {}) -> None:
        """
        Конструктор базового объект Script
        ! Может выбрасывать исключения: !
            * BaseSLLException
            * LockerSLLException
            * IncorrectInputParamSLLException
            * IncorrectNamespaceFlagsSLLException

        :param name: имя скрипта
        :param namespace: объект содержащий в себя аргументы командной строки
        :param options: словарь, содерит другие дополнительные опции для скрипта
        """
        if name is None:
            raise ScriptCriticalExceptionSLL(message="Input parameter 'name' is None", inst=None)
        if namespace is None:
            raise ScriptCriticalExceptionSLL(message="Input parameter 'namespace' is None", inst=None)
        self._script_name = name
        self._start_time = datetime.datetime.now()
        self._namespace = namespace
        self._options = options
        self._locker = None  # объект для работы с lock файлом
        self._ttl_lock_file = datetime.timedelta(hours=1)  # время жизни лок файла
        self._create_lock_file = create_lock_file

        if namespace.__contains__("verbose"):
            CLogger.config_logger(debug_info_on=namespace.verbose,
                                  email_color=namespace.verbose,
                                  terminate_critical_exception=options['options']["terminate_critical_exception"]
                                  if options and "terminate_critical_exception" in options['options'].keys()
                                  else True,
                                  custom_email_config=options['options']["custom_email_config"]
                                  if options and "custom_email_config" in options['options'].keys() else None,
                                  simple_email_config=options['options']["simple_email_config"]
                                  if options and "simple_email_config" in options['options'].keys() else None)

            if not namespace.verbose:
                if self._create_lock_file:
                    self._locker = Locker(self.path_lock_file, self.script_name)
                    self.lock()

        CLogger.info(f"Script name: {name}")
        CLogger.info(f"Script options: {options}")
        self.namespace_info()

        if namespace.__contains__("cfg_host"):
            if namespace.cfg_host:
                self.cfg = CFG(namespace.cfg_host)
                if not self.cfg:
                    raise ScriptCriticalExceptionSLL(f"Not found cfg_host file! cfg_host={namespace.cfg_host}", None)
            else:
                raise ScriptCriticalExceptionSLL(f"Namespace.cfg_host is None! cfg_host={namespace.cfg_host}", None)
        else:
            self.cfg_host = None

        self.DATE_TIME_FROM = None
        self.DATE_TIME_TO = None
        try:
            if namespace.__contains__("datetime_from") and namespace.__contains__("datetime_to"):
                if namespace.datetime_from and namespace.datetime_to:
                    self.DATE_TIME_FROM = datetime.datetime.strptime(namespace.datetime_from, "%Y-%m-%d-%H:%M:%S")
                    self.DATE_TIME_TO = datetime.datetime.strptime(namespace.datetime_to, "%Y-%m-%d-%H:%M:%S")
        except Exception as inst_datetime:
            raise ScriptCriticalExceptionSLL(inst=inst_datetime, message="Error while convert datetime!\n"
                                                                         " --datetime_from or --datetime_to")

        self.DATE_FROM = None
        self.DATE_TO = None
        try:
            if namespace.__contains__("date_from") and namespace.__contains__("date_to"):
                if namespace.date_from and namespace.date_to:
                    self.DATE_FROM = datetime.datetime.strptime(namespace.date_from, "%Y-%m-%d")
                    self.DATE_TO = datetime.datetime.strptime(namespace.date_to, "%Y-%m-%d")
        except Exception as inst_datetime:
            raise ScriptCriticalExceptionSLL(inst=inst_datetime, message="Error while convert datetime!\n"
                                                                         " --date_from or --date_to")

        if namespace.__contains__("date"):
            if namespace.date:
                try:
                    self.DATE = datetime.datetime.strptime(namespace.date, '%Y-%m-%d')
                except Exception as inst_datetime:
                    raise ScriptCriticalExceptionSLL(inst=inst_datetime,
                                                     message="Error while convert datetime! --date")
            else:
                self.DATE = default_date
        else:  # не убирать, условие небредовое!
            self.DATE = default_date  # важно! для всех скриптов, в которых вообще нет указания параметра date

        self.DATE_of_DATE = self.DATE.strftime('%Y-%m-%d')
        self.DAY = self.DATE.day
        self.MONTH = self.DATE.month

        CLogger.print(text=f"{Color.Magenta}DATE : {Color.Yellow}{self.DATE}")
        CLogger.print(text=f"{Color.Magenta}DATE_of_DATE : {Color.Yellow}{self.DATE_of_DATE}")
        CLogger.print(text=f"{Color.Magenta}DAY : {Color.Yellow}{self.DAY}")
        CLogger.print(text=f"{Color.Magenta}MONTH : {Color.Yellow}{self.MONTH}")
        if self.DATE_TIME_FROM and self.DATE_TIME_TO:
            CLogger.print(text=f"{Color.Magenta}DATE_TIME_FROM : {Color.Yellow}{self.DATE_TIME_FROM}")
            CLogger.print(text=f"{Color.Magenta}DATE_TIME_TO : {Color.Yellow}{self.DATE_TIME_TO}")
        if self.DATE_FROM and self.DATE_TO:
            CLogger.print(text=f"{Color.Magenta}DATE_FROM : {Color.Yellow}{self.DATE_FROM}")
            CLogger.print(text=f"{Color.Magenta}DATE_TO : {Color.Yellow}{self.DATE_TO}")

    def namespace_info(self):
        """
        Выводит в красивом виде агрументы командной строки, переданный в скрипт через переменную namespace
        :return: None
        """
        if self.namespace:
            CLogger.printc(text="System args:", color=Color.Magenta)
            for attr in self.namespace.__dict__:
                CLogger.print(text=f"--{Color.Magenta}{attr} : {Color.Yellow}{self.namespace.__dict__[attr]}")

    def start_info(self):
        CLogger.info(f"\nScript {self.script_name!r} Start!\n"
                     f"\tAt: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    def finish_success_info(self):
        CLogger.info(f"\nScript {self.script_name!r} {Color.Light_Green}Successful Finished!{CLogger.infoColor}\n"
                     f"\tAt: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                     f"\tTotal work time: {(datetime.datetime.now() - self.start_time)}\n")

    def finish_fail_info(self):
        CLogger.info(f"\nScript {self.script_name!r} {Color.Light_Red}FAILED Finished!{CLogger.infoColor}\n"
                     f"\tAt: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                     f"\tTotal work time: {(datetime.datetime.now() - self.start_time)}\n")

    def lock(self):
        self.locker.lock(timedelta_before_unlock=self.options['ttl_lock_file']) \
            if 'ttl_lock_file' in self.options else self.locker.lock(timedelta_before_unlock=self.ttl_lock_file)

    def unlock(self):
        self.locker.unlock()

    def __enter__(self):
        self.start_info()
        return self

    def __exit__(self, type_e, value_e, traceback_e):
        if type_e is None:
            self.finish_success_info()
            if self.namespace.__contains__('verbose'):
                if not self.namespace.verbose:
                    if self._create_lock_file:
                        self.unlock()
        else:
            self.finish_fail_info()
            CLogger.error(f"Failed finished Script: {self.script_name} with type: {type_e} and code: {value_e}")

    def profile(self, name_profile_block: str):
        return Profiler(self.namespace.verbose, name_profile_block) if self.namespace.__contains__('verbose') \
            else Profiler(True, name_profile_block)