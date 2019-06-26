import os


from .. import other
from ..logger import CLogger
from ..exeption import BaseExceptionSLL

"""
########################################################################################################################
######################             T E L E G R A M           ###########################################################
########################################################################################################################        
"""


def telegram_notify_bash(id_chat: int, msg: str) -> None:
    """
    Функция для отправки уведомления в Telegram чат от имени бота
    :param id_chat: ID Telegram чата
    :param msg: сообщение
    :return: None
    """
    try:
        os.system(f"./bash_recovery/telegram_bot_notify.sh {id_chat} {msg}")
    except Exception as inst:
        CLogger.exception(inst, f"Error while try send Telegram notify! In func: {other.who_am_i()!r}")


def telegram_notify_ssh(login: str, host: str, port: int, path_identity_file: str, path_remote_msg_script: str,
                        id_chat: int, msg: str) -> None:
    """
    Функция для отправки уведомления в Telegram чат от имени бота
    :param login: имя пользователя на сервере
    :param host: адресс хоста
    :param port: port ssh сервера
    :param path_identity_file: путь к приватному файлу ключа для ssh proxy сервера
    :param path_remote_msg_script: путь к скрипту на удаленном сервере для отправки сообщения
    :param id_chat:  ID Telegram чата
    :param msg: сообщение
    :return: None
    """
    try:
        os.system(f"ssh -p {port} -i {path_identity_file} {login}@{host} {path_remote_msg_script} {id_chat} {msg}")
    except Exception as inst:
        CLogger.exception(inst, f"Error while try send Telegram notify! In func: {other.who_am_i()!r}")


def ssh_remote_command(ssh_config: dict, command: str) -> None:
    """
    Выполнить shell команду на удаленном хосте
    :param ssh_config: настройки для подключения к хосту по ssh
    :param command: команда
    :return: None
    """
    try:
        if "identify_file" in ssh_config.keys():
            os.system(f"ssh -p {ssh_config['port']}"
                      f"-i {ssh_config['identify_file']} {ssh_config['user']}@{ssh_config['host']} '{command}'")
        elif "password" in ssh_config.keys():
            os.system(f"ssh -p {ssh_config['port']} {ssh_config['user']}@{ssh_config['host']} '{command}'")
        else:
            raise BaseExceptionSLL("No password or identify_file settings", None)
    except Exception as inst:
        CLogger.exception(inst, None)