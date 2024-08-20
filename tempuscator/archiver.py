import logging
import subprocess
from tempuscator.exceptions import BackupFileCorrupt, DirectoryNotEmpty
import os
import shutil

XBSTREAM_PATH = "/usr/bin/xbstream"
SCP_PATH = "/usr/bin/scp"
XTRABACKUP_PATH = "/usr/bin/xtrabackup"


class BackupProcessor():
    """
    Xtrabackup backup processor class
    """

    def __init__(
            self,
            source: str,
            target: str,
            parallel: int = 4,
            force: bool = False) -> None:
        self._logger = logging.getLogger("Tempuscator")
        self.target = target
        self.source = source
        self.force = force
        self.parallel = parallel
        if not os.path.isfile(self.source):
            raise FileNotFoundError(f"Backup {self.source} not found, or not regular file")
        if self.force:
            if os.path.exists(self.target):
                self._logger.debug(f"Removing {self.target}")
                shutil.rmtree(path=self.target)
        if os.path.isfile(path=self.target):
            raise FileExistsError(f"Destination {self.target} is regulara file, it should be empty dir or non existing path")
        if not os.path.exists(path=self.target):
            os.mkdir(self.target)
        if os.path.isdir(self.target):
            empty = os.listdir(path=self.target)
            if len(empty) != 0:
                raise DirectoryNotEmpty(f"Directory {self.target} not empty")

    def extract(self, debug: bool = False) -> None:
        """
        Extract xtrabackup backup file
        """
        self._logger.info(f"Extracting backup to {self.target}")
        cli = [XBSTREAM_PATH]
        cli.append("-x")
        cli.append("--directory")
        cli.append(self.target)
        cli.append("--parallel")
        cli.append(str(self.parallel))
        if debug:
            cli.append("--verbose")
        with open(self.source, 'r') as backup:
            self._logger.debug(f"executing: {' '.join(cli)}")
            extract = subprocess.Popen(cli, stdin=backup)
            extract.communicate()
            if not extract.returncode == 0:
                raise BackupFileCorrupt(f"File {self.source} looks like corruptted, try another")

    def prepare(self, debug: bool = False) -> None:
        """
        Prepare extracted backup
        """
        self._logger.info(f"Preparing restored backup in {self.target}")
        cli = [XTRABACKUP_PATH]
        cli.append("--prepare")
        cli.append("--target-dir")
        cli.append(self.target)
        self._logger.debug(f"Executing: {' '.join(cli)}")
        if debug:
            prepare = subprocess.Popen(cli)
            prepare.communicate()
            return
        prepare = subprocess.Popen(cli, stderr=subprocess.DEVNULL)
        prepare.wait()

    def decompress(self, debug: bool = False) -> None:
        """
        Decompress extracted files
        """
        cli = [XTRABACKUP_PATH]
        cli.append("--decompress")
        cli.append("--parallel")
        cli.append(str(self.parallel))
        cli.append("--remove-original")
        cli.append("--target-dir")
        cli.append(self.target)
        self._logger.info(f"executing: {' '.join(cli)}")
        if debug:
            decompress = subprocess.Popen(cli)
            decompress.wait()
            return
        decompress = subprocess.Popen(cli, stderr=subprocess.DEVNULL)
        decompress.wait()

    def create(
            self,
            dst: str,
            debug: bool = False,
            socket: str = "/var/lib/mysql/mysql.sock") -> None:
        """
        Create xtrabackup compressed archive (xbstream)
        """
        self._logger.info("Creating xbstream archive")
        if os.path.exists(dst):
            raise FileExistsError(f"Destination {dst} already exists, not overwriting")
        cli = [XTRABACKUP_PATH]
        cli.append("--backup")
        cli.append("--stream")
        cli.append("--compress")
        cli.append("--parallel")
        cli.append(str(self.parallel))
        cli.append("--compress-threads")
        cli.append(str(self.parallel))
        cli.append("--socket")
        cli.append(socket)
        self._logger.debug(f"Executing: {' '.join(cli)}")
        with open(dst, 'wb') as archive:
            if debug:
                backup = subprocess.Popen(cli, stdout=archive)
            else:
                backup = subprocess.Popen(cli, stdout=archive, stderr=subprocess.DEVNULL)
            backup.communicate()

    def cleanup(self) -> None:
        """
        Remove not needed files and rotate certificates
        """
        self._logger.info("Cleaning not needed files")
        files = [
            "auto.cnf",
            "backup-my.cnf",
            "ca-key.pem",
            "ca.pem",
            "client-cert.pem",
            "client-key.pem",
            "private_key.pem",
            "public_key.pem",
            "server-cert.pem",
            "server-key.pem",
            "xtrabackup_binlog_info",
            "xtrabackup_checkpoints",
            "xtrabackup_info",
            "xtrabackup_logfile",
            "xtrabackup_slave_info",
            "xtrabackup_tablespaces"
        ]
        for f in files:
            remove_file = os.path.join(self.target, f)
            self._logger.debug(f"Removing: {remove_file}")
            if os.path.isfile(remove_file):
                os.remove(remove_file)

    def uploader(
            self,
            host: str,
            user: str,
            src: str,
            dst: str,
            progress: bool = False) -> None:
        """
        Upload new archive to destination server
        """
        self._logger.info(f"Uploading file: {src} to {host}:{dst}")
        output = None
        cli = [SCP_PATH]
        cli.append(src)
        cli.append(f"{user}@{host}:{dst}")
        if not progress:
            output = subprocess.DEVNULL
        upload = subprocess.Popen(cli, stdout=output)
        upload.communicate()
