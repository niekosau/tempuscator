import dataclasses


@dataclasses.dataclass(frozen=True)
class Sentry():
    """
    Sentry data class for remote logging
    """
    dsn: str
    trace_sample_tate: float
    env: str

    def __post_init(self) -> None:
        import sentry_sdk
        sentry_sdk.init(
            dsn=self.dsn,
            trace_sample_tate=self.trace_sample_tate,
            environment=self.env
        )
