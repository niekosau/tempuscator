import logging

LOG_FORMAT_DEFAULT = "{name}: {message}"
LOG_FORMAT_DEBUG = "[{levelname:^7}] - {name}: {message}"


def init_logger(name: str, level: str = "info", file: str = None) -> logging.Logger:
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
    if file:
        fh = logging.FileHandler(file, "w")
        fh.setLevel(set_level)
        log_format = logging.Formatter(LOG_FORMAT_DEBUG, style="{")
        fh.setFormatter(log_format)
        logger.addHandler(fh)
        return
    con_logger = logging.StreamHandler()
    con_logger.setFormatter(log_format)
    logger.addHandler(con_logger)
