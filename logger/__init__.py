import logging
import sys

from .color import Color
from .logger import ColorLogger


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
