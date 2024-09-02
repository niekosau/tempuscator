# Executables

XBSTREAM_PATH = "/usr/bin/xbstream"
SCP_PATH = "/usr/bin/scp"
XTRABACKUP_PATH = "/usr/bin/xtrabackup"
MYSQLD_PATH = "/usr/sbin/mysqld"
PT_SHOW_GRANTS = "/usr/bin/pt-show-grants"
SYSTEMCTL_PATH = "/usr/bin/systemctl"
SSH_KEYSCAN_PATH = "/bin/ssh-keyscan"


# INotify masks
CLOSE_WRITE_MASK = 0x00000008

# Logging formats
LOG_FORMAT_DEFAULT = "{message}"
LOG_FORMAT_DEBUG = "[{levelname:^7}] - {name}: {message}"
LOG_FORMAT_FILE_DEFAULT = "{asctime}: " + LOG_FORMAT_DEFAULT
LOG_FORMAT_FILE_DEBUG = "{asctime} " + LOG_FORMAT_DEBUG
