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
        print("sentry initialized")
