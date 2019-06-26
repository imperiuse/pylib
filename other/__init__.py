import inspect
import json
import re


# Функция возращает имя вызывающей ее функции
def who_am_i(n=1, m=3):
    return inspect.stack()[n][m]


# Модуль поддержки json с комментариями
# Загрузка из файла
# 1. Игнорируем строки:
#     а) в начале которых есть   ignore_match     r'^[ ,    \n]*//+'
#     б) в которых есть          ignore_search     r'---+'
# 2. Вырезаем из строк которые не попали в п.1 все после
#                                split_zero_after  "//"
def load_comment_json(file, ignore_match=r'^[ ,    \n]*//+', ignore_search=r'---+', split_zero_after="//") -> any:
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

