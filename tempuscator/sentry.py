import dataclasses


@dataclasses.dataclass(frozen=True)
class Sentry():
    """
    Sentry data class for remote logging
    """
    dsn: str
    env: str

    def __post_init__(self) -> None:
        import sentry_sdk
        sentry_sdk.init(
            dsn=self.dsn,
            environment=self.env
        )


def init_sentry(path: str) -> None:
    """
    Sentry initialization
    """
    import configparser
    conf = configparser.RawConfigParser()
    with open(path, "r") as c_file:
        conf.read_file(c_file)
    if not conf.has_section("Sentry"):
        return
    Sentry(**conf["Sentry"])
