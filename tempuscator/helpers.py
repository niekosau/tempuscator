import sqlalchemy as db
import paramiko
import urllib
import logging
import os

_logger = logging.getLogger("Notifier")


def execute_query(engine: db.Engine, query: str, close: bool = False) -> None:
    """
    Executute raw query
    """
    with engine.connect() as conn:
        conn.execute(db.text(query))
        conn.commit()
    engine.dispose(close=close)


def scan_ssh_key(url: str = None, hostname: str = None, port: int = 22) -> None:
    if url:
        address = urllib.parse.urlparse(url=url)
        host = address.hostname
        port = address.port
        user = address.username
    if hostname:
        host = hostname
        user = os.environ["USER"]
    _logger.info(f"Scanning ssh key of {host}")
    ssh = paramiko.Transport(host, port)
    ssh.get_security_options().key_types = ["ssh-rsa"]
    ssh.connect(username=user)
    key = ssh.get_remote_server_key()
    ssh.close()
    know_host_file = os.path.join(os.environ["HOME"], ".ssh", "known_hosts")
    if not os.path.isfile(know_host_file):
        open(know_host_file, "w").close()
    hostfile = paramiko.HostKeys(filename=know_host_file)
    hostname = f"[{host}]:{port}" if port != 22 else f"{host}"
    hostfile.add(hostname=hostname, key=key, keytype=key.get_name())
    hostfile.save(filename=know_host_file)
