import logging
import git
import os
import shutil
from typing import List


_logger = logging.getLogger(__name__)


class Scruber():

    def __init__(self, url: str, dst: str, sql_file: str) -> None:
        self.dst = dst
        if os.path.isdir(self.dst):
            _logger.debug(f"{dst} exists, pulling changes")
            repo = git.Repo(path=self.dst)
            repo.remotes.origin.pull()
        else:
            _logger.debug(f"{dst} doesn't exist, cloning repo")
            repo = git.Repo.clone_from(url=url, to_path=self.dst)
        self.source_file = os.path.join(dst, sql_file)
        if not os.path.exists(self.source_file):
            raise FileNotFoundError(f"{self.source_file} file not found")

    def __del__(self) -> None:
        if os.path.isdir(self.dst):
            shutil.rmtree(self.dst)

    def get_queries(self) -> List[str]:
        with open(self.source_file, "r") as f:
            return f.read().split("\n")[:-1]
