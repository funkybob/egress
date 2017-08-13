from . import libpq


class Result:
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

    def __init__(self, result, connection):
        self._result = result
        self._conn = connection

    def status(self):
        return libpq.PQresultStatus(self._result)

    def status_text(self, status=None):
        if status is None:
            status = self.status()
        msg = libpq.PQresStatus(status)
        return msg.decode('utf-8')

    def error_message(self):
        msg = libpq.PQresultErrorMessage(self._result)
        return msg.decode('utf-8')

    def cmd_status(self):
        msg = libpq.PQcmdStatus(self._result)
        return msg.decode('utf-8') if msg else msg

    def clear(self):
        if self._result:
            libpq.PQclear(self._result)
        self._result = None

    def __del__(self):
        self.clear()

    def error_field(self, field):
        msg = libpq.PQresultErrorField(self._result, field)
        return msg.decode('utf-8') if msg else msg

    def nfields(self):
        return libpq.PQnfields(self._result)

    def ntuples(self):
        return libpq.PQntuples(self._result)

    def cmd_tuples(self):
        return libpq.PQcmdTuples(self._result)

    def field_type(self, field):
        return libpq.PQftype(self._result, field)

    def field_modifier(self, field):
        return libpq.PQfmod(self._result, field)

    def field_name(self, field):
        return libpq.PQfname(self._result, field).decode('utf-8')

    def field_size(self, field):
        return libpq.PQfsize(self._result, field)

    def get_value(self, row, field):
        return libpq.PQgetvalue(self._result, row, field)

    def get_length(self, row, field):
        return libpq.PQgetlength(self._result, row, field)

    def get_isnull(self, row, field):
        return libpq.PQgetisnull(self._result, row, field)


class PGConnection:
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

    def __init__(self, conn=None):
        self._conn = conn

    def connect(self, conn_str):
        self._conn = libpq.PQconnectdb(conn_str.encode('utf-8'))

    def version(self):
        return libpq.PQserverVersion(self._conn)

    def finish(self):
        if self._conn:
            libpq.PQfinish(self._conn)
        self._conn = None

    def __del__(self):
        self.finish()

    def reset(self):
        libpq.PQreset(self._conn)

    def pid(self):
        return libpq.PQbackendPID(self._conn)

    def status(self):
        return libpq.PQstatus(self._conn)

    def db(self):
        return libpq.PQdb(self._conn)

    def user(self):
        return libpq.PQuser(self._conn)

    def passwd(self):
        return libpq.PQpass(self._conn)

    def host(self):
        return libpq.PQhost(self._conn)

    def port(self):
        return libpq.PQport(self._conn)

    def tty(self):
        return libpq.PQtty(self._conn)

    def options(self):
        return libpq.PQoptions(self._conn)

    def transaction_status(self):
        return libpq.PQtransactionStatus(self._conn)

    def parameter_status(self, name):
        if isinstance(str, name):
            name = name.encode('utf-8')
        return libpq.PQparameterStatus(self._conn, name)

    def protocol_version(self):
        return libpq.PQprotocolVersion(self._conn)

    def server_version(self):
        return libpq.PQserverVersion(self._conn)

    def error_message(self):
        return libpq.PQerrorMessage(self._conn)

    # Execution
    def execute(self, command):
        result = libpq.PQexec(self._conn, command.encode('utf-8'))
        return Result(result, self)

    def exec_params(self, *args):
        result = libpq.PQexecParams(self._conn, *args)
        return Result(result, self)

    def prepare(self, name, query, nparams, param_types):
        result = libpq.PQprepare(name.encode('utf-8'), query.encode('utf-8'), nparams, param_types)
        return Result(result, self)
