import logging
import subprocess
import psutil
import dataclasses
import sqlalchemy as db
import os

MYSQLD_PATH = "/usr/sbin/mysqld"

_logger = logging.getLogger("Tempuscator")


@dataclasses.dataclass()
class MysqlData():

    datadir: str
    socket: str = dataclasses.field(init=False)
    pid: int = dataclasses.field(init=False)
    engine: db.Engine = dataclasses.field(init=False)
    running: bool = False

    def __post_init__(self) -> None:
        self.socket = os.path.join(self.datadir, "tempuscator.sock")
        url = f"mysql+pymysql://localhost/mysql?unix_socket={self.socket}"
        self.engine = db.create_engine(url=url)

    def __del__(self):
        """
        Destructor to stop mysqld on garbage collection
        """
        if self.running:
            self.stop()

    def start(self) -> None:
        """
        Start mysqld service
        """
        _logger.info("Starting mysqld")
        pid_path = os.path.join(self.datadir, "tempuscator.pid")
        cli = [MYSQLD_PATH]
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
        cli.append(f"--log-error={self.datadir}/tempuscator.err")
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
        mysqld = subprocess.Popen(cli, stdout=subprocess.DEVNULL)
        mysqld.wait()
        with open(pid_path, 'r') as f:
            pid = f.read()
        self.pid = int(pid)
        self.running = True

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
