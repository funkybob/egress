
from ctypes import cdll, c_int, c_uint, Structure, POINTER, c_char_p, c_char, c_size_t
from ctypes.util import find_library

libpq = cdll.LoadLibrary(find_library('pq'))

Oid = c_uint

# Connection status values
CONNECTION_OK = 0
CONNECTION_BAD = 1
# XXX The rest are for async connections
CONNECTION_STARTED = 2              # Waiting for connection to be made.
CONNECTION_MADE = 3                 # Connection OK; waiting to send.
CONNECTION_AWAITING_RESPONSE = 4    # Waiting for a response from the postmaster
CONNECTION_AUTH_OK = 5              # Received authentication; waiting for
                                    # backend startup.
CONNECTION_SETENV = 6               # Negotiating environment.
CONNECTION_SSL_STARTUP = 7          # Negotiating SSL.
CONNECTION_NEEDED = 8               # Internal state: connect() needed

ConnStatusType = c_int

# Result status values
PGRES_EMPTY_QUERY = 0       # empty query string was executed
PGRES_COMMAND_OK = 1        # a query command that doesn't return anything was
                            # executed properly by the backend
PGRES_TUPLES_OK = 2         # a query command that returns tuples was executed
                            # properly by the backend, PGresult contains the
                            # result tuples
PGRES_COPY_OUT = 3          # Copy Out data transfer in progress
PGRES_COPY_IN = 4           # Copy In data transfer in progress
PGRES_BAD_RESPONSE = 5      # an unexpected response was recv'd from the
                            # backend
PGRES_NONFATAL_ERROR = 6    # notice or warning message
PGRES_FATAL_ERROR = 7       # query failed
PGRES_COPY_BOTH = 8         # Copy In/Out data transfer in progress
PGRES_SINGLE_TUPLE = 9      # single tuple from larger resultset

ExecStatusType = c_int

# Transaction status values
PQTRANS_IDLE = 0        # connection idle */
PQTRANS_ACTIVE = 1      # command in progress */
PQTRANS_INTRANS = 2     # idle, within transaction block */
PQTRANS_INERROR = 3     # idle, within failed transaction */
PQTRANS_UNKNOWN = 4     # cannot determine status */


PG_DIAG_SEVERITY = ord('S')
PG_DIAG_SQLSTATE = ord('C')
PG_DIAG_MESSAGE_PRIMARY = ord('M')
PG_DIAG_MESSAGE_DETAIL = ord('D')
PG_DIAG_MESSAGE_HINT = ord('H')
PG_DIAG_STATEMENT_POSITION = 'P'
PG_DIAG_INTERNAL_POSITION = 'p'
PG_DIAG_INTERNAL_QUERY = ord('q')
PG_DIAG_CONTEXT = ord('W')
PG_DIAG_SOURCE_FILE = ord('F')
PG_DIAG_SOURCE_LINE = ord('L')
PG_DIAG_SOURCE_FUNCTION = ord('R')


TransStatusType = c_int


class PGconn(Structure):
    _fields_ = []


PGconn_p = POINTER(PGconn)


class PGresult(Structure):
    _fields_ = []


PGresult_p = POINTER(PGresult)

# PGconn *PQconnectdb(const char *conninfo);
PQconnectdb = libpq.PQconnectdb
PQconnectdb.argtypes = [c_char_p]
PQconnectdb.restype = PGconn_p

# void PQfinish(PGconn *conn);
PQfinish = libpq.PQfinish
PQfinish.argtypes = [PGconn_p]
PQfinish.restype = None

# void PQreset(PGconn *conn);
PQreset = libpq.PQreset
PQreset.argtypes = [PGconn_p]
PQreset.restype = None

# ConnStatusType PQstatus(const PGconn *conn);
PQstatus = libpq.PQstatus
PQstatus.argtypes = [PGconn_p]
PQstatus.restype = ConnStatusType

# PGresult *PQexec(PGconn *conn, const char *command);
PQexec = libpq.PQexec
PQexec.argtypes = [PGconn_p, c_char_p]
PQexec.restype = PGresult_p

# int PQserverVersion(const PGconn *conn);
PQserverVersion = libpq.PQserverVersion
PQserverVersion.argtypes = [PGconn_p]
PQserverVersion.restype = c_int

# PGTransactionStatusType PQtransactionStatus(const PGconn *conn);
PQtransactionStatus = libpq.PQtransactionStatus
PQtransactionStatus.argtypes = [PGconn_p]
PQtransactionStatus.restype = c_uint

# int PQbackendPID(const PGconn *conn);
PQbackendPID = libpq.PQbackendPID
PQbackendPID.argtypes = [PGconn_p]
PQbackendPID.restype = c_int

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
PQexecParams.restype = PGresult_p


# ExecStatusType PQresultStatus(const PGresult *res);
PQresultStatus = libpq.PQresultStatus
PQresultStatus.argtypes = [PGresult_p]
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
PQgetvalue.restype = POINTER(c_char)

# int PQgetisnull(const PGresult *res,
#                 int row_number,
#                 int column_number);
PQgetisnull = libpq.PQgetisnull
PQgetisnull.argtypes = [PGresult_p, c_int, c_int]
PQgetisnull.restype = c_int

# int PQgetlength(const PGresult *res,
#                 int tup_num,
#                 int field_num);
PQgetlength = libpq.PQgetlength
PQgetlength.argtypes = [PGresult_p, c_int, c_int]
PQgetisnull.restype = c_int


# char *PQresultErrorMessage(const PGresult *res);
PQresultErrorMessage = libpq.PQresultErrorMessage
PQresultErrorMessage.argtyes = [PGresult_p]
PQresultErrorMessage.restype = c_char_p


# char *PQresultErrorField(const PGresult *res, int)
PQresultErrorField = libpq.PQresultErrorField
PQresultErrorField.argtypes = [PGresult_p, c_int]
PQresultErrorField.restype = c_char_p

# char *PQcmdTuples(PGresult *res);
PQcmdTuples = libpq.PQcmdTuples
PQcmdTuples.argtypes = [PGresult_p]
PQcmdTuples.restype = c_char_p

# char *PQerrorMessage(const PGconn *conn);
PQerrorMessage = libpq.PQerrorMessage
PQerrorMessage.argtypes = [PGconn_p]
PQerrorMessage.restype = c_char_p

# char *PQescapeLiteral(PGconn *conn, const char *str, size_t length);
PQescapeLiteral = libpq.PQescapeLiteral
PQescapeLiteral.argtypes = [PGconn_p, c_char_p, c_size_t]
PQescapeLiteral.restype = c_char_p

# char *PQcmdStatus(PGresult *res);
PQcmdStatus = libpq.PQcmdStatus
PQcmdStatus.argtypes = [PGresult_p]
PQcmdStatus.restype = c_char_p


# char *PQresStatus(ExecStatusType status);
PQresStatus = libpq.PQresStatus
PQresStatus.argtypes = [c_int]
PQresStatus.restype = c_char_p
