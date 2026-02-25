import logging
from pythonjsonlogger import jsonlogger
from week1_basics.config import settings


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    # Avoid duplicate handlers if logger already configured
    if logger.hasHandlers():
        return logger

    # User log level from config
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logger.setLevel(log_level)

    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.propagate = False
    return logger
