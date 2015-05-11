
from .exceptions import *
from .connection import *
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
    return Connection(conn, **kwargs)

apilevel = '2.0'

threadsafety = 1

paramstyle = 'numeric'
