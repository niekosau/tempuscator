import argparse
import os
import logging


def base_args() -> argparse.ArgumentParser:
    """
    Base arguments
    """
    c_user_home = os.path.expanduser("~")
    default_config = os.path.join(c_user_home, ".tempuscator")
    log_levels = [level.lower() for level in logging._nameToLevel.keys()][:-1]
    args = argparse.ArgumentParser()
    base = args.add_argument_group(title="Base", description="Base parameters")
    base.add_argument(
        "--log-level",
        choices=log_levels,
        help=f"Log level from: {' '.join(log_levels)}, default %(default)s",
        default="info"
    )
    base.add_argument(
        "--log-file",
        help="Path to log file",
        type=str
    )
    base.add_argument(
        "--debug",
        help="enable debuging",
        action="store_true"
    )
    base.add_argument(
        "--force",
        help="Force remove and recreate target directory",
        action="store_true"
    )
    base.add_argument(
        "-c",
        "--config",
        help="Path to config file, default: %(default)s",
        default=default_config
    )
    return args


def obf_args() -> argparse.Namespace:
    """
    Arguments for obfuscation
    """
    args = base_args()
    ssh_args = args.add_argument_group(title="SSH", description="Arguments for uploading file")
    archiver = args.add_argument_group(title="Archiver")
    obfuscator = args.add_argument_group(title="Obfuscator")
    archiver.add_argument(
        "-b",
        "--backup-file",
        help="Path to file",
        required=True
    )
    archiver.add_argument(
        "--remove-backup",
        help="Remove backup after extracting",
        action="store_true"
    )
    archiver.add_argument(
        "--target-dir",
        help="Where to extract files",
        default="/tmp/obfuscation"
    )
    archiver.add_argument(
        "--save-archive",
        help="Path were to save obfuscated archive",
        required=True
    )
    archiver.add_argument(
        "-p",
        "--parallel",
        help="Parallel parameter for xtrabackup",
        default=4
    )
    obfuscator.add_argument(
        "--sql-file",
        help="Patgh to sql file",
        required=True
    )
    ssh_args.add_argument(
        "--host",
        type=str,
        help="Address of the server were to upload file"
    )
    ssh_args.add_argument(
        "--ssh-user",
        type=str,
        help="Username for ssh connection"
    )
    ssh_args.add_argument(
        "--scp-dst",
        type=str,
        help="File path were to put file"
    )
    return args.parse_args()


def swap_args() -> argparse.Namespace:
    """
    Arguments for swapper
    """
    args = base_args()
    archiver = args.add_argument_group(title="Archiver")
    swapper = args.add_argument_group(title="Swapper")
    mysql = args.add_argument_group(title="Mysql")
    mysql.add_argument(
        "--mysql-user",
        type=str,
        help="User for connecting to mysql"
    )
    mysql.add_argument(
        "--mysql-password",
        type=str,
        help="Password for connecting to mysql"
    )
    archiver.add_argument(
        "--user",
        help="User to which change permissions",
        type=str,
        default="mysql"
    )
    archiver.add_argument(
        "--group",
        help="Group for permission change",
        type=str,
        default="mysql"
    )
    archiver.add_argument(
        "-b",
        "--backup-file",
        help="Path to file",
        type=str,
        required=True
    )
    archiver.add_argument(
        "--extract-dir",
        help="Where to extract files, default: %(default)s",
        type=str,
        default="/tmp/swapper",
    )
    archiver.add_argument(
        "--remove-backup",
        help="Remove backup after extracting",
        action="store_true"
    )
    swapper.add_argument(
        "--backup",
        help="Leave backup directory of previuos mysql version",
        action="store_true"
    )
    return args.parse_args()


def notifier_args() -> argparse.Namespace:
    args = base_args()
    notifier = args.add_argument_group(title="Notifier")
    notifier.add_argument(
        "--watch-dir",
        help="Directory to watch, default: %(default)s",
        type=str,
        default="/tmp/notifier"
    )
    notifier.add_argument(
        "--action",
        help="Action to call on IN_CLOSE_WRITE",
        required=False,
        choices=["obfuscate", "swap"]
    )
    notifier.add_argument(
        "--conf-action",
        help="Action config file",
        type=str,
        required=True
    )
    return args.parse_args()
