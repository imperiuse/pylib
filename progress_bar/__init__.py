from ..logger import CLogger


class ProgressBar:
    """
    ProgressBar
    """
    __slots__ = ('_info_str', 'silence', 'max_cnt_value', 'counter', 'percents', 'one_percent', 'every_cnt_percent')

    @property
    def info_str(self):
        return self._info_str

    def __init__(self, info_str: str, max_cnt_value: int, every_cnt_percent: int = 1, silence: bool = False):
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

    def increment_counter_n(self, n: int):
        self.counter = self.counter + n - 1
        self.increment_counter()

    def increment_counter(self):
        self.counter += 1
        self.percents = self.counter // self.one_percent

        if (not self.silence) and (self.counter % (self.one_percent * self.every_cnt_percent) == 0):
            self.__say()

    def __say(self):
        CLogger.info(f"{self.info_str}. Percent:{self.percents}%%. (Counter = {self.counter})")