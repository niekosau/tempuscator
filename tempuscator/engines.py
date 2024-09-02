import logging
import subprocess
import psutil
import dataclasses
import sqlalchemy as db
import os
from typing import Union
from tempuscator.constants import MYSQLD_PATH


_logger = logging.getLogger(__name__)


@dataclasses.dataclass()
class MysqlData():

    datadir: str
    debug: bool
    user: Union[str, int] = dataclasses.field(default=os.getuid())
    group: Union[str, int] = dataclasses.field(default=os.getgid())
    mysql_user: str = dataclasses.field(default=os.environ["USER"])
    mysql_password: str = dataclasses.field(default=None)
    socket: str = dataclasses.field(init=False)
    pid: int = dataclasses.field(init=False, default=None)
    engine: db.Engine = dataclasses.field(init=False)
    running: bool = False
    conn_pool_size: int = dataclasses.field(default=4)

    def __post_init__(self) -> None:
        self.socket = os.path.join(self.datadir, "tempuscator.sock")
        url = ["mysql+pymysql://"]
        url.append(self.mysql_user)
        if self.mysql_password:
            url.append(":")
            url.append(self.mysql_password)
        url.append("@localhost/mysql?unix_socket=")
        url.append(self.socket)
        _logger.debug(f"Engine: {''.join(url)}")
        self.engine = db.create_engine(url="".join(url), echo=self.debug, pool_size=self.conn_pool_size)

    def __del__(self):
        """
        Destructor to stop mysqld on garbage collection
        """
        if self.running:
            self.stop()

    def start(self, skip_grants: bool = True) -> str:
        """
        Start mysqld service
        """
        _logger.info("Starting mysqld")
        pid_path = os.path.join(self.datadir, "tempuscator.pid")
        mysql_log = "/tmp/tempuscator.log"
        if not os.path.exists(mysql_log):
            with open(mysql_log, "w") as f:
                pass
        cli = [MYSQLD_PATH]
        if skip_grants:
            cli.append("--skip-grant-tables")
        cli.append("--datadir")
        cli.append(self.datadir)
        cli.append("--skip-networking")
        cli.append("--skip-name-resolve")
        cli.append("--skip-log-bin")
        cli.append("--socket")
        cli.append(self.socket)
        cli.append("--sync-binlog")
        cli.append("0")
        cli.append("--daemonize")
        cli.append("--pid-file")
        cli.append(pid_path)
        cli.append(f"--log-error={mysql_log}")
        cli.append("--sql-mode=")
        cli.append("--innodb-buffer-pool-instances=8")
        cli.append("--innodb-buffer-pool-size=6G")
        cli.append("--skip-innodb-doublewrite")
        cli.append("--innodb-flush-log-at-trx-commit=0")
        cli.append("--thread-pool-size=32")
        cli.append("--skip-performance-schema")
        cli.append("--skip-innodb-adaptive-hash-index")
        cli.append("--innodb-deadlock-detect=OFF")
        cli.append("--innodb-lock-wait-timeout=60")
        cli.append("--skip-innodb-buffer-pool-dump-at-shutdown")
        cli.append("--innodb-page-cleaners=8")
        cli.append("--innodb-log-buffer-size=128M")
        cli.append("--innodb-io-capacity=3000")
        cli.append("--innodb-io-capacity-max=6000")
        cli.append("--innodb-flush-neighbors=0")
        cli.append("--innodb-redo-log-capacity=4G")
        _logger.debug(f"Executing: {' '.join(cli)}")
        mysqld = subprocess.Popen(cli, stdout=subprocess.DEVNULL, user=self.user, group=self.group)
        mysqld.wait()
        with open(pid_path, 'r') as f:
            pid = f.read()
        self.pid = int(pid)
        self.running = True
        return self.socket

    def stop(self) -> None:
        """
        Stop mysqld service
        """
        if psutil.pid_exists(self.pid):
            _logger.info("Mysqld running, stopping")
            self.engine.dispose()
            proc = psutil.Process(pid=self.pid)
            proc.terminate()
            proc.wait()
            self.running = False
            return
        _logger.warning(f"Pid: {self.pid} doesn't exist")
