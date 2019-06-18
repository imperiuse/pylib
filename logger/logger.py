import smtplib
import ssl
from email.mime.text import MIMEText

from decorators import sll_exception_catcher
from exeption import EmailSendExceptionSLL
from exeption import SimpleEmailSendExceptionSLL
from .color import Color


COLOR = Color()


@sll_exception_catcher
def email_send(smtp_server, port, email, dest_email, password, subject, message):
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(email, password)
            server.sendmail(email, dest_email,
                            f"From: {email}\nTo: {dest_email}\nSubject: {subject}\n"
                            f"\n{message.encode('utf-8', errors='ignore')}")
    except Exception as inst:
        raise EmailSendExceptionSLL(message=f"Something went wrong while `email_send` to {dest_email}", inst=inst,
                                    email=email, dest_email=dest_email, email_message=message,
                                    smtp_server=smtp_server, port=port)


@sll_exception_catcher
def simple_email_send(email, dest_email, subject, message):
    try:
        msg = MIMEText(f'{message}')
        msg['Subject'] = f'{subject}'
        msg['From'] = f'{email}'
        msg['To'] = f'{", ".join(dest_email)}'
        s = smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
    except Exception as inst:
        raise SimpleEmailSendExceptionSLL(message=f"Something went wrong while `simple_email_send` to {dest_email}",
                                          inst=inst, email=email, dest_email=dest_email, email_message=message)



class ColorLogger:
    """
    Цветной логгер и с широким спектром возможностей.
    """
    Reset = "\x1b[0m"
    infoColor = COLOR.Green
    debugColor = COLOR.Yellow
    printColor = COLOR.Blue
    warningColor = COLOR.Yellow
    exceptionColor = COLOR.Red
    errorColor = COLOR.Red
    criticalColor = COLOR.Red
    debug_info_on = True
    emailColor = False
    terminate_critical_exception = True
    custom_email_config = None

    # Example
    # custom_email_config = {
    #       "smtp_server":"smtp.yandex.ru",
    #       "port" : 465,
    #       "email" : "from@ya.ru",
    #       "dest_email" : "to@ya.ru",
    #       "password" : "password",
    #       "subject" : "Analytics Scripts"}

    def __init__(self, logger,
                 info_color=COLOR.Green,
                 print_color=COLOR.Blue,
                 debug_color=COLOR.Yellow,
                 error_color=COLOR.Red):
        self.Logger = logger
        self.infoColor = info_color
        self.debugColor = debug_color
        self.printColor = print_color
        self.warningColor = debug_color
        self.exceptionColor = error_color
        self.errorColor = error_color
        self.criticalColor = error_color
        self.debug_info_on = True

    def config_logger(self, email_color=False, debug_info_on=True, info_color="\x1b[32m", debug_color="\x1b[33m",
                      error_color="\x1b[31m", terminate_critical_exception="True", custom_email_config=None):
        self.infoColor = info_color
        self.debugColor = debug_color
        self.errorColor = error_color
        self.debug_info_on = debug_info_on
        self.emailColor = email_color
        self.terminate_critical_exception = terminate_critical_exception
        self.custom_email_config = custom_email_config

    def custom_email_notify(self, message: str):
        if self.custom_email_config:
            email_send(**self.custom_email_config, message=message)

    def info(self, text: str):
        self.Logger.info(f"{self.infoColor}{text}{self.Reset}")

    def print(self, text: str):
        self.Logger.info(f"{self.printColor}{text}{self.Reset}")

    def printc(self, text: str, color: str):
        self.Logger.info(f"{color}{text}{self.Reset}")

    def debug(self, text: str):
        if self.debug_info_on:
            self.Logger.debug(f"{self.debugColor}{text}{self.Reset}")

    def warning(self, text: str):
        self.custom_email_notify(text)
        self.Logger.warning(f"{self.warningColor}{text}{self.Reset}" if self.emailColor else f"{text}")

    def error(self, text: str):
        self.custom_email_notify(text)
        self.Logger.error(f"{self.errorColor}{text}{self.Reset}" if self.emailColor else f"{text}")

    def critical_error(self, errcode: int, text: str):
        self.error(text)
        if self.terminate_critical_exception:
            exit(errcode)

    def exception(self, exception: any, text: str):
        self.custom_email_notify(f"{text}\n{exception}\n")
        self.Logger.error(f"{self.exceptionColor}{text}\n{exception}\n{self.Reset}"
                          if self.emailColor else f"{text}\n{exception}\n")

    def critical_exception(self, exception: any, text: str, err_code: int):
        self.exception(exception, text)
        if self.terminate_critical_exception:
            exit(err_code)