import logging
import subprocess
from tempuscator.exceptions import BackupFileCorrupt, DirectoryNotEmpty
from typing import Union
import os
import shutil
import pwd

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
            force: bool = False,
            remove_backup: bool = False,
            user: Union[str, int] = os.getuid(),
            group: Union[str, int] = os.getgid(),
            logger_name: str = "Obfuscator",
            save_archive: str = None) -> None:
        self._logger = logging.getLogger(logger_name)
        self._log_level = self._logger.getEffectiveLevel()
        self.target = target
        self.source = source
        self.force = force
        self.parallel = parallel
        self.user = user
        self.group = group
        self.remove_backup = remove_backup
        self.save_archive = save_archive
        if not os.path.isfile(self.source):
            raise FileNotFoundError(f"Backup {self.source} not found, or not regular file")
        if self.force:
            if os.path.exists(self.target):
                self._logger.debug(f"Removing {self.target}")
                shutil.rmtree(path=self.target)
            if os.path.exists(self.save_archive):
                self._logger.debug(f"Removing: {self.save_archive}")
                os.remove(self.save_archive)
        if os.path.isfile(path=self.target):
            raise FileExistsError(f"Destination {self.target} is regulara file, it should be empty dir or non existing path")
        if os.path.exists(self.save_archive):
            raise FileExistsError(f"Destination {self.save_archive} already exists, not overwriting")
        if not os.path.exists(path=self.target):
            os.umask(0)
            os.makedirs(name=self.target, mode=0o751)
            if isinstance(self.user, str):
                user = pwd.getpwnam(self.user)
                os.chown(path=self.target, uid=user.pw_uid, gid=user.pw_gid)
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
            self._logger.debug(f"Executing: {' '.join(cli)}")
            extract = subprocess.Popen(cli, stdin=backup, user=self.user, group=self.group)
            extract.communicate()
            if not extract.returncode == 0:
                raise BackupFileCorrupt(f"File {self.source} looks like corruptted, try another")
        if self.remove_backup and extract.returncode == 0:
            log_msg = f"Removing {self.source}" if self._log_level <= 10 else "Removing source backup"
            self._logger.log(self._log_level, log_msg)
            os.remove(self.source)

    def prepare(self, debug: bool = False) -> None:
        """
        Prepare extracted backup
        """
        output = None if debug else subprocess.DEVNULL
        self._logger.info(f"Preparing restored backup in {self.target}")
        cli = [XTRABACKUP_PATH]
        cli.append("--prepare")
        cli.append("--target-dir")
        cli.append(self.target)
        self._logger.debug(f"Executing: {' '.join(cli)}")
        prepare = subprocess.Popen(cli, stderr=output, user=self.user, group=self.group)
        prepare.wait()

    def decompress(self, debug: bool = False) -> None:
        """
        Decompress extracted files
        """
        output = None if debug else subprocess.DEVNULL
        self._logger.info("Decompressing files")
        cli = [XTRABACKUP_PATH]
        cli.append("--decompress")
        cli.append("--parallel")
        cli.append(str(self.parallel))
        cli.append("--remove-original")
        cli.append("--target-dir")
        cli.append(self.target)
        self._logger.debug(f"Executing: {' '.join(cli)}")
        decompress = subprocess.Popen(cli, stderr=output, user=self.user, group=self.group)
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
        self._logger.debug(f"Force: {self.force}")
        output = None if debug else subprocess.DEVNULL
        if self.force and os.path.exists(dst):
            self._logger.warning(f"Removing {dst}")
            os.remove(dst)
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
            backup = subprocess.Popen(cli, stdout=archive, stderr=output, user=self.user, group=self.group)
            backup.communicate()

    def cleanup_backup_files(self) -> None:
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
        output = None if progress else subprocess.DEVNULL
        cli = [SCP_PATH]
        cli.append(src)
        cli.append(f"{user}@{host}:{dst}")
        upload = subprocess.Popen(cli, stdout=output, user=self.user, group=self.group)
        upload.communicate()

    def cleanup(self) -> None:
        """
        Remove target directory and all files inside
        """
        self._logger.info("Cleaning up")
        self._logger.debug(f"Removing: {self.target}")
        shutil.rmtree(self.target)
