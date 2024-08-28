import sqlalchemy as db
import logging
import threading
from tempuscator.helpers import execute_query
from tempuscator.repo import Scruber
import json

_logger = logging.getLogger(__name__)


class Obfuscator():

    def __init__(self, scrub: Scruber) -> None:
        self.queries = scrub.get_queries()

    def __str__(self) -> str:
        return json.dumps(self.__dict__)

    def change_system_user_password(self, user: str, engine: db.Engine, empty: bool = False) -> None:
        password = ""
        if not empty:
            password = "somepassword"
        meta = db.MetaData()
        meta.reflect(bind=engine)
        USER = meta.tables["user"]
        with engine.connect() as conn:
            query = USER.update().where(USER.c.User == user).values(authentication_string=password)
            conn.execute(query)
            conn.commit()

    def cleanup_system_users(self, engine: db.Engine) -> None:
        _logger.info("Cleaning up users")
        meta = db.MetaData()
        meta.reflect(bind=engine)
        USER = meta.tables["user"]
        query = db.delete(
            USER
        ).filter(
            db.not_(
                USER.c.User.in_([
                    "root",
                    "mysql.sys",
                    "mysql.infoschema",
                    "mysql.session"])
                )
            )
        with engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def mask(self, engine: db.Engine) -> None:
        _logger.info("Executing masking queries")
        threads = []
        for q in self.queries:
            threads.append(threading.Thread(target=execute_query, args=(engine, q, )))
        for t in threads:
            t.start()
        for j in threads:
            j.join()
