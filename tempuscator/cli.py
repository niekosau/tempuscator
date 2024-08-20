import os
import time
from tempuscator.archiver import BackupProcessor
from tempuscator.executor import Obfuscator
from tempuscator.engines import MysqlData
from tempuscator.helpers import init_sentry, arguments, init_logger


def main() -> None:
    """
    Entry point for tool
    """
    args = arguments()
    if os.path.isfile(args.config):
        _logger.info(f"Initializing sentry from {args.config}")
        init_sentry(path=args.config)
    _logger.debug(args)
    mysql = MysqlData(
        datadir=args.target_dir
    )
    backup = BackupProcessor(
        source=args.backup,
        target=mysql.datadir,
        force=args.force,
        parallel=args.parallel
    )
    obfuscator = Obfuscator(source=args.sql_file)
    backup.extract()
    xtrabackup_info = os.path.join(mysql.datadir, "xtrabackup_info")
    if not os.path.isfile(xtrabackup_info):
        backup.decompress()
    backup.prepare()
    backup.cleanup()
    try:
        mysql.start()
        obfuscator.cleanup_system_users(engine=mysql.engine)
        obfuscator.change_system_user_password(engine=mysql.engine, user="root", empty=True)
        obfuscator.mask(engine=mysql.engine)
        backup.create(socket=mysql.socket, dst=args.save_archive)
        backup.uploader(
            host=args.host,
            user=args.user,
            src=args.save_archive,
            dst=args.scp_dst)
    finally:
        mysql.stop()


if __name__ == "__main__":
    _logger = init_logger(name="Tempuscator", level="info", )
    _logger.info(f"Starting {__name__}")
    start = time.perf_counter()
    main()
    stop = time.perf_counter()
    execution_time = round((stop - start)/60, 2)
    _logger.info(f"Program took: {execution_time} minutes")
