from logger import CLogger, Color


class MicroMock(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


# TODO Класс для помощи в тестировании (mocking)
class MockingHelper:
    def __init__(self):
        pass

    @staticmethod
    def get_cursor():
        return MockingCursorDB()


# TODO Класс фиктивный cursor к БД
class MockingCursorDB:
    lastrowid = None

    def __init__(self):
        pass

    @staticmethod
    def execute(self, sql: str):
        CLogger.printc(text=sql, color=Color.Magenta)

    @staticmethod
    def executemany(self, sql: str, data: any):
        CLogger.printc(text=sql, color=Color.Magenta)
        CLogger.printc(text=data, color=Color.Blue)