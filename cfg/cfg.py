import xml.etree.cElementTree as eTree

from logger import CLogger

class CFG:
    """
     Класс для работы с cfg_host.xml - файлом конфигуратором модуля
    """

    def __init__(self,
                 path="./cfg_host.xml",
                 tree=None
                 ):
        self.path = path
        try:
            if not tree:
                self.tree = eTree.parse(path)
            else:
                self.tree = tree
        except Exception as inst:
            CLogger.exception(inst, f"Error while INIT_ETREE(). Path:{path}")
            raise inst

    def get_root(self):
        return self.tree.getroot()

    # Получение строчки конфига сервера (поиск по параметру name) из xml
    def get_server_config_by_name(self, name):
        root = self.get_root()
        for server in root.find('MYSQL').iter('SERVER'):
            if server.attrib['name'] == name:
                return server.attrib

    # Получение строчки конфига сервера (поиск по параметру name) из xml
    def get_redis_server_config_by_name(self, name):
        root = self.get_root()
        for server in root.find('CACHE').iter('SERVER'):
            if server.attrib['name'] == name:
                return server.attrib

    # Извлечение и формирование конфига для DB
    def get_db_config_by_name(self, name):
        server_config = self.get_server_config_by_name(name)
        return {
            "host": server_config["ip"],
            "port": server_config["port"],
            "user": server_config["login"],
            "pass": server_config["pass"],
            "name_db": None
        }

    # Извлечение и формирование конфига для DB
    def get_db_config_by_name_with_db_name(self, name, db_name):
        config = self.get_db_config_by_name(name)
        config["name_db"] = db_name
        return config

    # Извлечение и формирование конфига для redis
    def get_redis_config_by_name_server(self, name):
        server_config = self.get_redis_server_config_by_name(name)
        return {
            "Host": server_config["url"],
            "Port": server_config["port"],
            "DB": server_config["dbname"],
            "Password": server_config["pass"],
        }
