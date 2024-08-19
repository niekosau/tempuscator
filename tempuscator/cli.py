import logging
import argparse
import os
from tempuscator.exceptions import DirectoryNotEmpty, BackupFileCorrupt
import subprocess
import shutil
import psutil
import sqlalchemy
import threading
import time

XBSTREAM_PATH = "/usr/bin/xbstream"
XTRABACKUP_PATH = "/usr/bin/xtrabackup"
MYSQLD_PATH = "/usr/sbin/mysqld"


def init_sentry(path: str) -> None:
    import configparser
    from tempuscator.sentry import Sentry
    conf = configparser.RawConfigParser()
    with open(path, "r") as c_file:
        conf.read_file(c_file)
    if not conf.has_section("Sentry"):
        _logger.warning(f"Config {path} doesn't contains [Sentry] section")
        return
    Sentry(**conf["Sentry"])


def arguments() -> argparse.Namespace:
    c_user_home = os.path.expanduser("~")
    default_config = os.path.join(c_user_home, ".tempuscator")
    args = argparse.ArgumentParser()
    args.add_argument(
        "-b",
        "--backup",
        help="Path to file",
        required=True
    )
    args.add_argument(
        "--target-dir",
        help="Where to extract files",
        default="/tmp/obfuscation"
    )
    args.add_argument(
        "--force",
        help="Force remove and recreate target directory",
        action="store_true"
    )
    args.add_argument(
        "--debug",
        help="Enable debuging output",
        action="store_true"
    )
    args.add_argument(
        "--sql-file",
        help="Patgh to sql file",
        required=True
    )
    args.add_argument(
        "-c",
        "--config",
        help=f"Path to config file, default: {default_config}",
        default=default_config
    )
    args.add_argument(
        "--save-file",
        help="Path were to save obfuscated archive",
        required=True
    )
    args.add_argument(
        "-p",
        "--parallel",
        help="Parallel parameter for xtrabackup",
        default=4
    )
    return args.parse_args()


def check_destination(
        path: str,
        recreate: bool) -> None:
    _logger.info("Checking destination")
    if recreate:
        _logger.warning(f"removing {path}")
        shutil.rmtree(path=path)
    if not os.path.exists(path=path):
        os.mkdir(path=path)
        return
    if os.path.isfile(path=path):
        raise FileExistsError(f"Destination {path} is regulara file, it should be empty dir or non existing path")
    if os.path.isdir:
        empty = os.listdir(path=path)
        if len(empty) != 0:
            raise DirectoryNotEmpty(f"Directory {path} not empty")


def extract_xbstream(
        source: str,
        destination: str,
        debug: bool,
        parallel: int = 4) -> None:
    cli = [XBSTREAM_PATH]
    cli.append("-x")
    cli.append("--directory")
    cli.append(destination)
    cli.append("--parallel")
    cli.append(str(parallel))
    if debug:
        cli.append("--verbose")
    with open(source, 'r') as backup:
        _logger.debug(f"executing: {' '.join(cli)}")
        extract = subprocess.Popen(cli, stdin=backup)
        extract.communicate()
        if not extract.returncode == 0:
            raise BackupFileCorrupt(f"File {source} looks like corruptted, try another")


def xtrabackup_prepare(
        target: str,
        debug: bool) -> None:
    cli = [XTRABACKUP_PATH]
    cli.append("--prepare")
    cli.append("--target-dir")
    cli.append(target)
    _logger.info(f"Executing: {' '.join(cli)}")
    if debug:
        prepare = subprocess.Popen(cli)
        prepare.communicate()
        return
    prepare = subprocess.Popen(cli, stderr=subprocess.DEVNULL)
    prepare.wait()


def xtrabackup_decompress(
        target: str,
        debug: bool,
        parallel: int = 4) -> None:
    cli = [XTRABACKUP_PATH]
    cli.append("--decompress")
    cli.append("--parallel")
    cli.append(str(parallel))
    cli.append("--remove-original")
    cli.append("--target-dir")
    cli.append(target)
    _logger.info(f"executing: {' '.join(cli)}")
    if debug:
        decompress = subprocess.Popen(cli)
        decompress.wait()
        return
    decompress = subprocess.Popen(cli, stderr=subprocess.DEVNULL)
    decompress.wait()


def parse_sql(source: str) -> list:
    with open(source, 'r') as f:
        queries = f.read().split("\n")
    return queries[:-1]


def start_mysqld(target: str) -> (int, str):
    pid_path = os.path.join(target, "tempuscator.pid")
    socket_path = os.path.join(target, "tempuscator.sock")
    cli = [MYSQLD_PATH]
    cli.append("--skip-grant-tables")
    cli.append("--datadir")
    cli.append(target)
    cli.append("--skip-networking")
    cli.append("--skip-name-resolve")
    cli.append("--skip-log-bin")
    cli.append("--socket")
    cli.append(socket_path)
    cli.append("--sync-binlog")
    cli.append("0")
    cli.append("--daemonize")
    cli.append("--pid-file")
    cli.append(pid_path)
    cli.append(f"--log-error={target}/tempuscator.err")
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
    _logger.info(f"Executing: {' '.join(cli)}")
    mysqld = subprocess.Popen(cli, stdout=subprocess.DEVNULL)
    mysqld.wait()
    with open(pid_path, 'r') as f:
        pid = f.read()
    return int(pid), socket_path


def stop_mysqld(pid: int) -> None:
    if psutil.pid_exists(pid):
        _logger.info("Mysqld running, stopping")
        proc = psutil.Process(pid=pid)
        proc.terminate()
        proc.wait()
        return
    _logger.warning(f"Pid: {pid} doesn't exist")


def create_db_engine(socket: str, p_size: int, debug: bool) -> sqlalchemy.Engine:
    return sqlalchemy.create_engine(
        url=f"mysql+pymysql://localhost/mysql?unix_socket={socket}",
        echo=debug,
        pool_size=p_size)


def mask_data(engine: sqlalchemy.Engine, queries: list, debug: bool) -> None:
    threads = []
    for q in queries:
        threads.append(threading.Thread(target=execute_query, args=(engine, q, )))
    for t in threads:
        t.start()
    for j in threads:
        j.join()


def execute_query(engine: sqlalchemy.Engine, query: str) -> None:
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(query))
        conn.commit()
    engine.dispose(close=False)


def cleanup_upsers(engine: sqlalchemy.Engine) -> None:
    _logger.info("Cleaning up users")
    meta = sqlalchemy.MetaData()
    meta.reflect(bind=engine)
    USER = meta.tables["user"]
    query = sqlalchemy.delete(
        USER
    ).filter(
        sqlalchemy.not_(
            USER.c.User.in_([
                "root",
                "mysql.sys",
                "mysql.infoschema",
                "mysql.session"])
            )
        )
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()


def change_passwords(engine: sqlalchemy.Engine) -> None:
    meta = sqlalchemy.MetaData()
    meta.reflect(bind=engine)
    USER = meta.tables["user"]
    users = [
        "root"
    ]
    with engine.connect() as conn:
        for u in users:
            query = USER.update().where(USER.c.User == u).values(authentication_string="")
            conn.execute(query)
            conn.commit()


def cleanup_files(path: str) -> None:
    _logger.info("Removing original cert files")
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
        remove_file = os.path.join(path, f)
        _logger.debug(f"Removing: {remove_file}")
        if os.path.isfile(remove_file):
            os.remove(remove_file)


def create_archive(destination: str, socket: str, debug: bool, parallel: int = 4) -> None:
    _logger.info("Creating xbstream archive")
    if os.path.exists(destination):
        raise FileExistsError(f"Destination {destination} already exists, not overwriting")
    cli = [XTRABACKUP_PATH]
    cli.append("--backup")
    cli.append("--stream")
    cli.append("--compress")
    cli.append("--parallel")
    cli.append(str(parallel))
    cli.append("--compress-threads")
    cli.append(str(parallel))
    cli.append("--socket")
    cli.append(socket)
    _logger.debug(f"Executing: {' '.join(cli)}")
    with open(destination, 'wb') as archive:
        if debug:
            backup = subprocess.Popen(cli, stdout=archive)
        else:
            backup = subprocess.Popen(cli, stdout=archive, stdrrr=subprocess.DEVNULL)
        backup.communicate()


def main() -> None:
    args = arguments()
    if os.path.isfile(args.config):
        _logger.info(f"Initializing sentry from {args.config}")
        init_sentry(path=args.config)
    _logger.debug(args)
    if not os.path.isfile(args.backup):
        raise FileNotFoundError(f"Backup {args.backup} not found, or not regular file")
    if not os.path.split(args.sql_file):
        raise FileNotFoundError(f"{args.sql_file} doesn't exist or not a regular file")
    if args.force:
        check_destination(path=args.target_dir, recreate=args.force)
        extract_xbstream(source=args.backup, destination=args.target_dir, debug=args.debug)
        xtrabackup_info = os.path.join(args.target_dir, "xtrabackup_info")
        if not os.path.isfile(xtrabackup_info):
            xtrabackup_decompress(target=args.target_dir, debug=args.debug)
        xtrabackup_prepare(target=args.target_dir, debug=args.debug)
    queries = parse_sql(source=args.sql_file)
    try:
        cleanup_files(path=args.target_dir)
        mysql_pid, mysql_socket = start_mysqld(target=args.target_dir)
        sql_conn = create_db_engine(socket=mysql_socket, p_size=len(queries), debug=args.debug)
        cleanup_upsers(engine=sql_conn)
        change_passwords(engine=sql_conn)
        mask_data(engine=sql_conn, queries=queries, debug=args.debug)
        create_archive(destination=args.save_file, socket=mysql_socket, debug=args.debug)
    finally:
        sql_conn.dispose()
        stop_mysqld(pid=mysql_pid)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    _logger = logging.getLogger(__name__)
    _logger.info(f"Starting {__name__}")
    start = time.perf_counter()
    main()
    stop = time.perf_counter()
    execution_time = round((stop - start)/60, 2)
    _logger.info(f"Program took: {execution_time} minutes")
