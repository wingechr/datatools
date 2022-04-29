import getpass
import socket


def get_user_host():
    return "%s@%s" % (get_user(), get_host())


def get_host():
    return socket.gethostname()


def get_user():
    return getpass.getuser()
