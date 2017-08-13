
from .exceptions import *  # NOQA
from .connection import Connection
from .types import *  # NOQA

from . import wrap

# Module Interface


def connect(**kwargs):
    '''
    Constructor for creating a connection to the database.

    Returns a Connection Object. It takes a number of parameters which are
    database dependent.
    '''
    conn_str = ' '.join([
        '='.join(item)
        for item in kwargs.items()
        if item[1]
    ])
    conn = wrap.PGConnection()
    conn.connect(conn_str)
    if conn.status() == conn.CONNECTION_BAD:
        msg = conn.error_message()
        raise OperationalError(msg)
    return Connection(conn, **kwargs)

apilevel = '2.0'  # NOQA

threadsafety = 1

paramstyle = 'numeric'


Binary = bytes