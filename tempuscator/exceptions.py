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


class BackupCreateError(Exception):
    """
    Exception for failed backup create
    """


class MyCnfConfigError(Exception):
    """
    Mysql user config exception
    """


class MissingConfigSection(Exception):
    """
    Exception for missing config section in ini file
    """


class NotARoot(Exception):
    """
    Exception if running user not a root
    """
