import logging
import traceback
from pathlib import Path


class Logger:
    @staticmethod
    def _get_log_path():
        log_dir = Path(__file__).resolve().parent / "log"
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir / "app.log"

    @staticmethod
    def __set_logger():
        log_path = Logger._get_log_path()
        logger = logging.getLogger("ppabe_logger")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        if logger.hasHandlers():
            logger.handlers.clear()

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    @classmethod
    def add_to_log(cls, level, message):
        try:
            logger = cls.__set_logger()

            if level == "critical":
                logger.critical(message)
            elif level == "debug":
                logger.debug(message)
            elif level == "error":
                logger.error(message)
            elif level == "info":
                logger.info(message)
            elif level == "warn":
                logger.warning(message)
            else:
                logger.info(message)
        except Exception as ex:
            print(traceback.format_exc())
            print(ex)