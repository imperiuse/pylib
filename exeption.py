import traceback


class BaseExceptionSLL(Exception):
    def __init__(self, message: str, inst: any):
        # Call the base class constructor with the parameters it needs
        stack = ''.join(traceback.format_stack()[:-2][:3])
        # [:-2] - убираем в списке вызов этой команды и вызов конструктора
        # [:3]  - ограничение глубины стека до 3-х строк
        super(BaseExceptionSLL, self).__init__(f"{message}\nStack trace:\n{stack}\nBase inst:\n\t{inst}")

        # Now for your custom code...
        self.inst = inst


class LockerCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(LockerCriticalExceptionSLL, self).__init__(f"Locker!\n{message}", inst)


class ScriptCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(ScriptCriticalExceptionSLL, self).__init__(f"Script!\n{message}", inst)


class DbCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(DbCriticalExceptionSLL, self).__init__(f"DB!\n{message}", inst)


class DemultiplexorCriticalExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(DemultiplexorCriticalExceptionSLL, self).__init__(f"Demultiplexor!\n{message}", inst)


class DemultiplexorExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any):
        super(DemultiplexorExceptionSLL, self).__init__(f"Demultiplexor!\n{message}", inst)


class DbSqlQueryExceptionSLL(BaseExceptionSLL):
    def __init__(self, db_info: str, sql: str, sql_args_data: any, message: str, inst: any):
        message = f"Problem with SQL query.\n" \
            f"Description:\n" \
            f"\t{message}\n" \
            f"DB info:\n" \
            f"\t{db_info}\n" \
            f"SQL:\n" \
            f"\t{sql}\n" \
            f"SQL Args:\n" \
            f"\t{sql_args_data}"
        super(DbSqlQueryExceptionSLL, self).__init__(message, inst)


class EmailSendExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any,
                 email: any, dest_email: any, smtp_server: any, port: any, email_message: any):
        super(EmailSendExceptionSLL, self).__init__(f"EmailSender!\n{message}", inst)
        self.info = f"\nSMPT: {smtp_server}:{port}\nFrom: {email}, To: {dest_email}\n{email_message}"


class SimpleEmailSendExceptionSLL(BaseExceptionSLL):
    def __init__(self, message: str, inst: any, email: any, dest_email: any, email_message: any):
        super(SimpleEmailSendExceptionSLL, self).__init__(f"SimpleEmailSender!\n{message}", inst)
        self.info = f"From: {email}, To: {dest_email}\n{email_message}"