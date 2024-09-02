import logging
import os
import configparser
import json
import inotify.adapters
import uuid
import time
from tempuscator.executor import Obfuscator
from tempuscator.engines import MysqlData
from tempuscator.swapper import SwapDirs
from tempuscator.exceptions import MissingConfigSection
from tempuscator.archiver import BackupProcessor
from tempuscator.repo import Scruber
from tempuscator.constants import CLOSE_WRITE_MASK

_logger = logging.getLogger(__name__)


class Watcher():

    def __init__(
            self,
            config: str,
            path: str,
            swapper: SwapDirs = None,
            mysql: MysqlData = None,
            obfuscator: Obfuscator = None,
            debug: bool = False) -> None:
        self.debug = debug
        self.path = path
        self.swapper = swapper
        self.mysql = mysql
        self.obfuscator = obfuscator
        if not os.path.isfile(config):
            raise FileNotFoundError(f"config file {config} not found!")
        if os.path.isfile(self.path):
            raise FileExistsError(f"{self.path} is regural file")
        if not os.path.exists(self.path):
            _logger.warning(f"Target directory {self.path} doesn't exist, creating")
            os.mkdir(path=self.path, mode=0o0750)
        raw_conf = configparser.RawConfigParser()
        with open(config, 'r') as f:
            raw_conf.read_file(f)
        if not raw_conf.has_section("obfuscator"):
            raise MissingConfigSection("Configuration missing section: obfuscator")
        self.conf: dict = raw_conf.__dict__.get("_sections")["obfuscator"]
        _logger.debug(f"config:\n{json.dumps(self.conf, indent=2)}")

    def watch_obfuscate(self) -> None:
        _logger.info("Starting obfuscator watcher")
        watcher = inotify.adapters.InotifyTree(path=self.path, mask=CLOSE_WRITE_MASK)
        for e in watcher.event_gen(yield_nones=False):
            (_, event, path, file) = e
            _logger.debug(f"received event: {e}")
            if "IN_CLOSE_WRITE" not in event:
                continue
            backup_file = os.path.join(path, file)
            _logger.info(f"Received: {backup_file}")
            self.__run_obfuscate(backup=backup_file)

    def _random_str(self) -> str:
        return uuid.uuid4().hex[:8]

    def __run_obfuscate(self, backup: str) -> None:
        start = time.perf_counter()
        repo_url = self.conf.get("repo")
        repo_dst = os.path.join("/tmp/", self._random_str())
        scrub_file = self.conf.get("scrub_sql")
        tmp_path = self.conf["tmp_path"] if "tmp_path" in self.conf.keys() else os.path.join("/tmp/", self._random_str())
        _logger.debug(f"Tmp path: {tmp_path}")
        processor = BackupProcessor(source=backup, target=tmp_path)
        _logger.debug(f"Backup procesor: {processor}")
        mysql = MysqlData(datadir=tmp_path, debug=self.debug)
        _logger.debug(f"Mysql data: {mysql}")
        scruber = Scruber(url=repo_url, dst=repo_dst, sql_file=scrub_file)
        obfuscator = Obfuscator(scrub=scruber)
        _logger.debug(f"Obfuscator: {obfuscator}")
        save_path = self.conf.get("save_path")
        try:
            processor.extract(debug=self.debug)
            processor.prepare(debug=self.debug)
            processor.cleanup_backup_files()
            socket = mysql.start()
            obfuscator.cleanup_system_users(engine=mysql.engine)
            obfuscator.change_system_user_password(engine=mysql.engine, user="root", empty=True)
            obfuscator.mask(engine=mysql.engine)
            processor.create(dst=save_path, socket=socket, debug=self.debug)
        finally:
            if socket:
                mysql.stop()
            processor.cleanup()
            _logger.debug(f"Removing: {backup}")
            os.remove(backup)
            end = time.perf_counter()
            execution_time = round((end - start)/60, 2)
            _logger.info(f"Execution time: {execution_time}")
        if "scp_host" in self.conf.keys():
            _logger.info("Uploading obfuscated backup")
            host = self.conf.get("scp_host")
            _logger.debug(f"Uploading to {host}")
            user = self.conf.get("ssh_user") if "ssh_user" in self.conf.keys() else os.environ["USER"]
            dst_path = self.conf.get('scp_path') if "scp_path" in self.conf.keys() else save_path
            processor.uploader(host=host, user=user, src=save_path, dst=dst_path)
