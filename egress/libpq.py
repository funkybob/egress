
from ctypes import *


libpq = cdll.LoadLibrary('pq')

class PGconn(Structure):
    _fields_ = []

PGconn_p = POINTER(PGconn)

# PGconn *PQconnectdb(const char *conninfo);
PQconnectdb = libpq.PQConnectdb
PQconnectdb.argtypes = [c_char_p]
PQconnectdb.restype = PGconn_p
