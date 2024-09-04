import logging
from logging.handlers import RotatingFileHandler
from tempuscator.constants import (
    LOG_FORMAT_DEFAULT,
    LOG_FORMAT_FILE_DEFAULT,
    LOG_FORMAT_DEBUG,
    LOG_FORMAT_FILE_DEBUG
)


def init_logger(name: str, level: str = "info", file: str = None) -> list:
    """
    Default logger initialization

    :param str name: Logger name for initialization
    :param str level: Root logging level, default info
    """
    log_levels = list(logging._nameToLevel.keys())[:-1]
    if level.upper() not in log_levels:
        raise ValueError(f"Log level {level} unknow")
    logger = logging.getLogger(name)
    set_level = logging.getLevelName(level.upper())
    log_format = logging.Formatter(LOG_FORMAT_DEBUG if set_level <= 10 else LOG_FORMAT_DEFAULT, style="{")
    logger.setLevel(set_level)
    con_logger = logging.StreamHandler()
    con_logger.setFormatter(log_format)
    con_logger.setLevel(set_level)
    logger.addHandler(con_logger)
    if file:
        log_format = logging.Formatter(LOG_FORMAT_FILE_DEBUG if set_level <= 10 else LOG_FORMAT_FILE_DEFAULT, style="{")
        rotating = RotatingFileHandler(filename=file, maxBytes=10240, mode='a', backupCount=3)
        rotating.setFormatter(log_format)
        rotating.setLevel(set_level)
        logger.addHandler(rotating)
