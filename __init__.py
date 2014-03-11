
from .exceptions import *
from .connection import *
from .types import *

# Module Interface

def connect(*args, **kwargs):
    '''
    Constructor for creating a connection to the database.

    Returns a Connection Object. It takes a number of parameters which are database dependent.
    '''

apilevel = '2.0'

threadsafety = 1

paramstyle = 'numeric'
