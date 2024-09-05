import dataclasses
import psutil
import os
import subprocess
from tempuscator.exceptions import MysqldNotRunning, MysqlAccessDeniend, MyCnfConfigError
from tempuscator.helpers import execute_query
from typing import List
import sqlalchemy as db
import logging
import datetime
import shutil
import configparser
from tempuscator.constants import (
    PT_SHOW_GRANTS,
    SYSTEMCTL_PATH
)

_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SwapDirs():
    src_dir: str
    user: str = dataclasses.field(default=None)
    password: str = dataclasses.field(default=None, repr=False)
    grants: List[str] = dataclasses.field(init=False, default_factory=list, repr=False)
    dst_dir: str = dataclasses.field(default="/var/lib/mysql")
    backup: bool = dataclasses.field(default=False)
    mysqld_running: bool = dataclasses.field(default=False)

    def __post_init__(self):
        if not os.path.exists(PT_SHOW_GRANTS):
            raise FileNotFoundError(f"{PT_SHOW_GRANTS} not found!")
        self.mysqld_running = "mysqld" in (p.name() for p in psutil.process_iter())
        if not self.mysqld_running:
            raise MysqldNotRunning("Mysqld not running")
        u_my_cnf = os.path.join(os.environ["HOME"], ".my.cnf")
        if os.path.exists(u_my_cnf):
            _logger.debug(f"Parsing {u_my_cnf}")
            u_conf = configparser.ConfigParser()
            with open(u_my_cnf, "r") as my_cnf:
                u_conf.readfp(my_cnf)
            if not u_conf.has_section("client"):
                raise MyCnfConfigError("User config doesn't have section client")
            if u_conf.has_option("client", "user"):
                self.user = u_conf.get("client", "user")
                _logger.debug(f"Mysql user: {self.user}")
            if u_conf.has_option("client", "password"):
                self.password = u_conf.get("client", "password") if u_conf.has_option("client", "password") else None
        cli = [PT_SHOW_GRANTS]
        cli.append("--database")
        cli.append("mysql")
        cli.append("--host")
        cli.append("localhost")
        cli.append("--ignore")
        cli.append("root@localhost")
        if self.user:
            cli.append("--user")
            cli.append(self.user)
        if self.password:
            cli.append("--password")
            cli.append(self.password)
        exec = subprocess.Popen(cli, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = exec.communicate()
        if err:
            raise MysqlAccessDeniend("Unable to connect to mysql")
        perms = out.decode().split("\n")[:-1]
        self.grants = [p for p in perms if not p.startswith("--")]

    def update_users(self, engine: db.Engine) -> None:
        _logger.info("Adding users")
        for grant in self.grants:
            execute_query(engine=engine, query=grant)
        _logger.info("Updating root password")
        query = db.text("ALTER USER {user}@localhost IDENTIFIED WITH caching_sha2_password BY '{password}'".format(
            user=self.user,
            password=self.password))
        with engine.connect() as conn:
            conn.execute(query)
            conn.commit()
        engine.dispose()

    def stop_mysqld(self) -> None:
        _logger.info("Stopping system Mysqld")
        if not self.mysqld_running:
            raise MysqldNotRunning("System mysqld not running")
        cli = [SYSTEMCTL_PATH]
        cli.append("stop")
        cli.append("mysqld")
        exec = subprocess.Popen(cli)
        exec.communicate()
        if exec.returncode == 0:
            self.mysqld_running = False

    def start_mysqld(self) -> None:
        _logger.info("Starting system Mysqld")
        if self.mysqld_running:
            return
        cli = [SYSTEMCTL_PATH]
        cli.append("start")
        cli.append("mysqld")
        exec = subprocess.Popen(cli)
        exec.communicate()
        if exec.returncode == 0:
            self.mysqld_running = True

    def swap_dirs(self) -> None:
        _logger.info("Swapping system Mysqld directories")
        if self.backup:
            _logger.info("Saving old directory")
            self.__backup_old_mysqld_directory()
        if os.path.exists(self.dst_dir):
            _logger.debug(f"Removing {self.dst_dir}")
            shutil.rmtree(path=self.dst_dir)
        _logger.debug(f"Moving {self.src_dir} to {self.dst_dir}")
        os.rename(self.src_dir, self.dst_dir)

    def __backup_old_mysqld_directory(self) -> None:
        _logger.debug("Creating backup of old mysqld directory")
        backup_suffix = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        new_name = f"{self.dst_dir}-{backup_suffix}"
        _logger.debug(f"New name: {new_name}")
        os.rename(self.dst_dir, new_name)
