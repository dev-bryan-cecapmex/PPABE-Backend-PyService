import logging
import os
import traceback

class Logger:
    
    @staticmethod
    def __set_logger():
        log_directory = os.path.join('src', 'utils', 'log')
        log_filename = 'app.log'

        # Crear carpeta si no existe
        os.makedirs(log_directory, exist_ok=True)

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        log_path = os.path.join(log_directory, log_filename)
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            "%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)

        # Evitar duplicar handlers
        if logger.hasHandlers():
            logger.handlers.clear()

        logger.addHandler(file_handler)

        return logger
    
    @classmethod
    def add_to_log(cls, level, message):
        try:
            logger = cls.__set_logger()

            match level.lower():
                case "critical":
                    logger.critical(message)
                case "debug":
                    logger.debug(message)
                case "error":
                    logger.error(message)
                case "info":
                    logger.info(message)
                case "warn" | "warning":
                    logger.warning(message)
                case _:
                    logger.info(f"UNKNOWN LEVEL: {level} | {message}")

        except Exception as ex:
            print(traceback.format_exc())
            print(ex)
