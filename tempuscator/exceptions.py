class DirectoryNotEmpty(Exception):
    """
    Directory not empty exception
    """


class BackupFileCorrupt(Exception):
    """
    Backup file corrupt exception
    """


class MysqldNotRunning(Exception):
    """
    Mysqld process not running exception
    """


class MysqlAccessDeniend(Exception):
    """
    Mysql access deniend exception
    """


class MyCnfConfigError(Exception):
    """
    Mysql user config exception
    """
