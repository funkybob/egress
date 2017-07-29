
from .exceptions import *
from .connection import Connection
from .types import *

from . import libpq

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
    conn = libpq.PQconnectdb(conn_str.encode('utf-8'))
    status = libpq.PQstatus(conn)
    if status == libpq.CONNECTION_BAD:
        msg = libpq.PQerrorMessage(conn)
        raise OperationalError(msg)
    return Connection(conn, **kwargs)

apilevel = '2.0'  # NOQA

threadsafety = 1

paramstyle = 'numeric'
