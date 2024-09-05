import os
import time
import logging
from tempuscator.archiver import BackupProcessor
from tempuscator.executor import Obfuscator
from tempuscator.engines import MysqlData
from tempuscator.logger import init_logger
from tempuscator.arguments import obf_args, swap_args, notifier_args
from tempuscator.sentry import init_sentry
from tempuscator.swapper import SwapDirs
from tempuscator.base import Watcher


def obfuscator() -> None:
    """
    Entry point for tool
    """
    args = obf_args()
    log_level = "debug" if args.debug else args.log_level
    init_logger(name="tempuscator", level=log_level, )
    _logger = logging.getLogger(__name__)
    _logger.debug("Starting Obfuscator")
    _logger.debug(args)
    start = time.perf_counter()
    if os.path.isfile(args.config):
        _logger.debug(f"Initializing sentry from {args.config}")
        init_sentry(path=args.config)
    mysql = MysqlData(
        datadir=args.target_dir,
        debug=args.debug
    )
    backup = BackupProcessor(
        source=args.backup_file,
        target=mysql.datadir,
        force=args.force,
        parallel=args.parallel,
        remove_backup=args.remove_backup,
        save_archive=args.save_archive
    )
    obfuscator = Obfuscator(source=args.sql_file)
    backup.extract(debug=args.debug)
    xtrabackup_info = os.path.join(mysql.datadir, "xtrabackup_info")
    if not os.path.isfile(xtrabackup_info):
        backup.decompress(debug=args.debug)
    backup.prepare(debug=args.debug)
    backup.cleanup_backup_files()
    try:
        mysql.start()
        obfuscator.cleanup_system_users(engine=mysql.engine)
        obfuscator.change_system_user_password(engine=mysql.engine, user="root", empty=True)
        obfuscator.mask(engine=mysql.engine)
        backup.create(socket=mysql.socket, dst=args.save_archive, debug=args.debug)
    finally:
        mysql.stop()
        backup.cleanup()
    backup.uploader(
        host=args.host,
        user=args.ssh_user,
        src=args.save_archive,
        dst=args.scp_dst,
        progress=args.debug)
    stop = time.perf_counter()
    execution_time = round((stop - start)/60, 2)
    _logger.info(f"Program took: {execution_time} minutes")


def swapper() -> None:
    args = swap_args()
    log_level = "debug" if args.debug else args.log_level
    init_logger(name="tempuscator", level=log_level)
    _logger = logging.getLogger(__name__)
    _logger.debug(f"ARGS: {args}")
    start = time.perf_counter()
    if os.path.isfile(args.config):
        _logger.debug(f"Initializing sentry from {args.config}")
        init_sentry(path=args.config)
    swapper = SwapDirs(
            src_dir=args.extract_dir,
            user=args.mysql_user,
            password=args.mysql_password,
            backup=args.backup)
    _logger.debug(f"Swapper: {swapper}")
    backup = BackupProcessor(
            source=args.backup_file,
            target=swapper.src_dir,
            force=args.force,
            user=args.user,
            group=args.group,
            logger_name="Swapper",
            remove_backup=args.remove_backup)
    _logger.debug(f"Backup processor: {backup}")
    updated_data = MysqlData(
            datadir=swapper.src_dir,
            debug=args.debug,
            user=args.user,
            group=args.group)
    _logger.debug(f"Mysql data: {updated_data}")
    backup.extract(debug=args.debug)
    backup.prepare(debug=args.debug)
    try:
        updated_data.start(skip_grants=False)
        swapper.update_users(updated_data.engine)
    finally:
        updated_data.stop()
    try:
        swapper.stop_mysqld()
        swapper.swap_dirs()
    finally:
        swapper.start_mysqld()
    stop = time.perf_counter()
    execution_time = round((stop - start)/60, 2)
    _logger.info(f"Program took: {execution_time} minutes")


def mysql_obf_watcher() -> None:
    args = notifier_args()
    if args.log_file:
        init_logger(name="tempuscator", level=args.log_level, file=args.log_file)
    else:
        init_logger(name="tempuscator", level=args.log_level)
    _logger = logging.getLogger(__name__)
    _logger.info("Starting inotify")
    _logger.debug(f"ARGS: {args}")
    if os.path.isfile(args.config):
        _logger.debug(f"Initializing sentry from {args.config}")
        init_sentry(path=args.config)
    listener = Watcher(config=args.conf_action, path=args.watch_dir, debug=args.debug)
    listener.watch_obfuscate()


def mysql_swap_watch() -> None:
    """
    Cli entry point for mysql directory wacher
    """
    args = notifier_args()
    if args.log_file:
        init_logger(name="tempuscator", level=args.log_level, file=args.log_file)
    else:
        init_logger(name="tempuscator", level=args.log_level)
    _logger = logging.getLogger(__name__)
    _logger.info("Starting inotify")
    _logger.debug(f"ARGS: {args}")
    if os.path.isfile(args.config):
        _logger.debug(f"Initializing sentry from {args.config}")
        init_sentry(path=args.config)
    listener = Watcher(config=args.conf_action, path=args.watch_dir, debug=args.debug)
    listener.watch(action="swap")
