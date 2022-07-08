import getpass
import logging  # noqa
import socket
import subprocess as sp


def get_user():
    """Return current user name"""
    return getpass.getuser()


def get_host():
    """Return current host name"""
    return socket.gethostname()


def get_user_host():
    return "%s@%s" % (get_user(), get_host())


def get_git_commit(cwd=None):
    proc = sp.Popen(["git", "rev-parse", "HEAD"], cwd=cwd, stdout=sp.PIPE)
    stdout, _ = proc.communicate()
    assert proc.returncode == 0
    return stdout.decode().strip()
