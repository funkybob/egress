
from ctypes import *


libpq = cdll.LoadLibrary('libpq.dylib')

class PGconn(Structure):
    _fields_ = []

PGconn_p = POINTER(PGconn)

# PGconn *PQconnectdb(const char *conninfo);
PQconnectdb = libpq.PQconnectdb
PQconnectdb.argtypes = [c_char_p]
PQconnectdb.restype = PGconn_p


PQfinish = libpq.PQfinish
PQfinish.argtypes = [PGconn_p]
PQfinish.restype = None

ConnStatusType = c_int

CONNECTION_OK = 0
CONNECTION_BAD = 1

# ConnStatusType PQstatus(const PGconn *conn);
PQstatus = libpq.PQstatus
PQstatus.argtypes = [PGconn_p]
PQstatus.restype = ConnStatusType

class PGresult(Structure):
    _fields_ = []

PGresult_p = POINTER(PGresult)

Oid = c_int

# PGresult *PQexec(PGconn *conn, const char *command);
PQexec = libpq.PQexec
PQexec.argtypes = [PGconn_p, c_char_p]
PQexec.restype = PGresult_p

# PGresult *PQexecParams(PGconn *conn,
#                        const char *command,
#                        int nParams,
#                        const Oid *paramTypes,
#                        const char * const *paramValues,
#                        const int *paramLengths,
#                        const int *paramFormats,
#                        int resultFormat);
PQexecParams = libpq.PQexecParams
PQexecParams.argtypes = [PGconn_p,
                        c_char_p,
                        c_int,
                        POINTER(Oid),
                        POINTER(c_char_p),
                        POINTER(c_int),
                        POINTER(c_int),
                        c_int,
                        ]


# Result status values
PGRES_EMPTY_QUERY = 0
PGRES_COMMAND_OK = 1
PGRES_TUPLES_OK = 2
PGRES_COPY_OUT = 3
PGRES_COPY_IN = 4
PGRES_BAD_RESPONSE = 5
PGRES_NONFATAL_ERROR = 6
PGRES_FATAL_ERROR = 7

ExecStatusType = c_int

# ExecStatusType PQresultStatus(const PGresult *res);
PQresultStatus = libpq.PQresultStatus
PQresultStatus.argtypes = [PGconn_p]
PQresultStatus.restype = ExecStatusType


# int PQnfields(const PGresult *res);
PQnfields = libpq.PQnfields
PQnfields.argtypes = [PGresult_p]
PQnfields.restype = c_int


# void PQclear(PGresult *res);
PQclear = libpq.PQclear
PQclear.argtypes = [PGresult_p]
PQclear.restype = None

# Oid PQftype(const PGresult *res,
#             int column_number);
PQftype = libpq.PQftype
PQftype.argtypes = [PGresult_p, c_int]
PQftype.restype = Oid

# int PQfmod(const PGresult *res,
#            int column_number);
PQfmod = libpq.PQfmod
PQfmod.argtypes = [PGresult_p, c_int]
PQfmod.restype = c_int

# char *PQfname(const PGresult *res,
#               int column_number);
PQfname = libpq.PQfname
PQfname.argtypes = [PGresult_p, c_int]
PQfname.restype = c_char_p

# int PQfsize(const PGresult *res,
#             int column_number);
PQfsize = libpq.PQfsize
PQfsize.argtypes = [PGresult_p, c_int]
PQfsize.restype = c_int

# int PQntuples(const PGresult *res);
PQntuples = libpq.PQntuples
PQntuples.argtypes = [PGresult_p]
PQntuples.restype = c_int

# char *PQgetvalue(const PGresult *res,
#                  int row_number,
#                  int column_number);
PQgetvalue = libpq.PQgetvalue
PQgetvalue.argtypes = [PGresult_p, c_int, c_int]
PQgetvalue.restype = c_char_p

# int PQgetisnull(const PGresult *res,
#                 int row_number,
#                 int column_number);
PQgetisnull = libpq.PQgetisnull
PQgetisnull.argtypes = [PGresult_p, c_int, c_int]
PQgetisnull.restype = c_int
