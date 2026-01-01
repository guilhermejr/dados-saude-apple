
import logging
import os
from logging.handlers import RotatingFileHandler

class Log:
    def __init__(self, log_dir=None):
        # Diretório para armazenar logs
        self.log_dir = os.path.dirname(os.path.abspath(__file__)) + "/logs"

        # Formato padrão
        self.formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

        # Remove qualquer handler padrão (como console)
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

        # Cria loggers separados
        self.debug = self._criar_logger("DEBUG", "debug.log", logging.DEBUG)
        self.info = self._criar_logger("INFO", "info.log", logging.INFO)
        self.error = self._criar_logger("ERROR", "error.log", logging.ERROR)

    def _criar_logger(self, nome, arquivo, nivel):
        logger = logging.getLogger(nome)
        logger.setLevel(nivel)

        if not logger.handlers:
            handler = RotatingFileHandler(self.log_dir +"/"+ arquivo, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
            handler.setLevel(nivel)
            handler.setFormatter(self.formatter)
            logger.addHandler(handler)

        return logger
