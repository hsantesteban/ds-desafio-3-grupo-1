from datetime import datetime as dt


class InMemoryLogger:

    def __init__(self):
        pass

    def info(self, msg: str):
        print(f'INFO - {str(dt.now())} - {msg}')

    def error(self, msg: str):
        print(f'ERROR - {str(dt.now())} - {msg}')
