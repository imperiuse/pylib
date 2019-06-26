import time

from ..logger import CLogger


class Profiler:
    """
    Профайлер времени выполнения участка скрипта
    """
    __slots__ = ('on', 'info_str', 'startTime')

    def __init__(self, on: bool = False, info_str: str = ""):
        self.info_str = info_str
        self.on = on

    def __enter__(self):
        if self.on:
            self.startTime = time.perf_counter()

    def __exit__(self, type_e, value_e, traceback_e):
        if self.on:
            CLogger.info(f"{self.info_str} Time execute block: {time.perf_counter() - self.startTime:.5f} sec")
