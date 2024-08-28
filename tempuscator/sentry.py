import dataclasses
import logging
import configparser
import sentry_sdk

_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class Sentry():
    """
    Sentry data class for remote logging

    :param str dsn: Sentry dsn
    :param str env: Env name
    :return: None
    """
    dsn: str
    env: str

    def __post_init__(self) -> None:
        _logger.debug(f"Initializing sentry with dsn: {self.dsn} and env: {self.env}")
        sentry_sdk.init(
            dsn=self.dsn,
            environment=self.env
        )


def init_sentry(path: str) -> None:
    """
    Sentry initialization

    :param str path: path to config file containing sentry dsn and env
    :return: None
    """
    conf = configparser.RawConfigParser()
    with open(path, "r") as c_file:
        conf.read_file(c_file)
    if not conf.has_section("Sentry"):
        return
    Sentry(**conf["Sentry"])
