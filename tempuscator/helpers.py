import sqlalchemy as db
import logging

_logger = logging.getLogger(__name__)


def execute_query(engine: db.Engine, query: str, close: bool = False) -> None:
    """
    Executute raw query
    """
    with engine.connect() as conn:
        conn.execute(db.text(query))
        conn.commit()
    engine.dispose(close=close)
