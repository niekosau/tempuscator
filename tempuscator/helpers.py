import logging
import sqlalchemy as db
import os
import argparse

LOG_FORMAT = "%(asctime)s - %(message)s"

_logger = logging.getLogger("Tempuscator")


def init_logger(name: str, level: str = "info") -> logging.Logger:
    """
    Default logger initialization
    """
    log_levels = list(logging._nameToLevel.keys())[:-1]
    log_format = logging.Formatter(LOG_FORMAT)
    if level.upper() not in log_levels:
        raise ValueError(f"Log level {level} unknow")
    logger = logging.getLogger(name)
    set_level = logging.getLevelName(level.upper())
    logger.setLevel(set_level)
    con_logger = logging.StreamHandler()
    con_logger.setFormatter(log_format)
    logger.addHandler(con_logger)
    return logger


def init_sentry(path: str) -> None:
    """
    Sentry initialization
    """
    import configparser
    from tempuscator.sentry import Sentry
    conf = configparser.RawConfigParser()
    with open(path, "r") as c_file:
        conf.read_file(c_file)
    if not conf.has_section("Sentry"):
        _logger.warning(f"Config {path} doesn't contains [Sentry] section")
        return
    Sentry(**conf["Sentry"])


def execute_query(engine: db.Engine, query: str, threaded: bool = True) -> None:
    """
    Executute raw query
    """
    with engine.connect() as conn:
        conn.execute(db.text(query))
        conn.commit()
    engine.dispose(close=not threaded)


def arguments() -> argparse.Namespace:
    """
    Arguments for cli
    """
    c_user_home = os.path.expanduser("~")
    default_config = os.path.join(c_user_home, ".tempuscator")
    args = argparse.ArgumentParser()
    args.add_argument(
        "-b",
        "--backup",
        help="Path to file",
        required=True
    )
    args.add_argument(
        "--target-dir",
        help="Where to extract files",
        default="/tmp/obfuscation"
    )
    args.add_argument(
        "--force",
        help="Force remove and recreate target directory",
        action="store_true"
    )
    args.add_argument(
        "--debug",
        help="Enable debuging output",
        action="store_true"
    )
    args.add_argument(
        "--sql-file",
        help="Patgh to sql file",
        required=True
    )
    args.add_argument(
        "-c",
        "--config",
        help=f"Path to config file, default: {default_config}",
        default=default_config
    )
    args.add_argument(
        "--save-archive",
        help="Path were to save obfuscated archive",
        required=True
    )
    args.add_argument(
        "-p",
        "--parallel",
        help="Parallel parameter for xtrabackup",
        default=4
    )
    ssh_args = args.add_argument_group(title="SSH", description="Arguments for uploading file")
    ssh_args.add_argument(
        "--host",
        type=str,
        help="Address of the server were to upload file"
    )
    ssh_args.add_argument(
        "--user",
        type=str,
        help="Username for ssh connection"
    )
    ssh_args.add_argument(
        "--scp-dst",
        type=str,
        help="File path were to put file"
    )
    return args.parse_args()
