from logger import CLogger

"""
########################################################################################################################
######################         APPLE         HELP        FUNC         ##################################################
########################################################################################################################

"""


def get_apple_category_id(name_category: str):
    """
    Декодирует имя категории в id
    :param name_category: <str> рус, eng text
    :return: <int> - номер категории apple
    """
    category_eng = {
        "Business": 6000,
        "Weather": 6001,
        "Utilities": 6002,
        "Travel": 6003,
        "Sports": 6004,
        "Social Networking": 6005,
        "Reference": 6006,
        "Productivity": 6007,
        "Photo & Video": 6008,
        "News": 6009,
        "Navigation": 6010,
        "Music": 6011,
        "Lifestyle": 6012,
        "Health & Fitness": 6013,
        "Games": 6014,
        "Finance": 6015,
        "Entertainment": 6016,
        "Education": 6017,
        "Books": 6018,
        "Medical": 6020,
        "Catalogs": 6022,
        "Food & Drink": 6023
    }
    category_rus = {
        "Бизнес": 6000,
        "Погода": 6001,
        "Утилиты": 6002,
        "Путешествия": 6003,
        "Спорт": 6004,
        "Социальные сети": 6005,
        "Справочники": 6006,
        "Производительность": 6007,
        "Фото и видео": 6008,
        "Новости": 6009,
        "Навигация": 6010,
        "Музыка": 6011,
        "Образ жизник": 6012,
        "Здоровье и Фитнес": 6013,
        "Игры": 6014,
        "Финансы": 6015,
        "Развлечения": 6016,
        "Образование": 6017,
        "Книги": 6018,
        "Медицина": 6020,
        "Каталоги": 6022,
        "Еда и напитки": 6023
    }
    return category_eng[name_category] if name_category in category_eng.keys() \
        else category_rus[name_category] if name_category in category_rus.keys() \
        else None


def get_app_data_from_itunes(app_id: int, search_by_lang: bool = False, lang_list: [] = None,
                             random_user_agent: bool = True) -> {}:
    """
    Функция получения данных (словарь) по указанному приложения из itunes
    :param app_id: уникальный id приложения в магазине itunes (обязательный параметр)
    :param search_by_lang: использовать уточнение страны поиск в запросе (необязательный параметр)
    :param lang_list: список стран для которых пытаться получить данные(необязательный параметр)
    :param random_user_agent: (необязательный флаг)
    :return: {....} data of app or empty {}
    """

    default_language_list = [
        "",
        "ru",
        "us",
        "gb",
        "de",
        "fr",
        "es",
        "ca",
        "tr",
        "cn"
    ]
    url_lookup_app_id = f"https://itunes.apple.com/lookup?id={app_id}"
    url_lookup_lang_app_id = f"https://itunes.apple.com/%s/lookup?id={app_id}"
    search_by_lang = search_by_lang if lang_list is None else True
    for lang in default_language_list if lang_list is None else lang_list:
        utf8_html, err = http_request(
            url=url_lookup_app_id if not search_by_lang else url_lookup_lang_app_id % lang,
            headers={'User-Agent': get_random_user_agent() if random_user_agent else None},
        )
        if err is None and utf8_html:
            try:
                j = json.loads(utf8_html)  # парсим получаемые данные
                if j["resultCount"]:  # result count > 1
                    return j["results"][0]  # берем первый результат
            except TypeError as inst1:
                CLogger.exception(inst1, f"TypeError while parse json file\n")
            except Exception as inst2:
                CLogger.exception(inst2, f"Unexpected Error while parse json file\n")
        else:
            CLogger.debug(f"Error while getting data from http request:\n {err}\n")
        if not search_by_lang:  # Однакратный запуск цикла, ищем просто, без стран, хитро хак
            break
    else:
        CLogger.debug(f"Not found data for this app: {app_id}\n")
        return {}
    return {}
