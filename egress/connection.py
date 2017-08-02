
from . import libpq
from .cursor import Cursor
from . import exceptions


EXC_MAP = {
    # Class 0A - Feature Not Supported
    b'0A': exceptions.NotSupportedError,

    # Class 21 - Cardinality Violation
    b'21': exceptions.ProgrammingError,
    # Class 22 - Data Exception
    b'22': exceptions.DataError,
    # Class 23 - Integrity Constraint Violation
    b'23': exceptions.IntegrityError,
    # Class 24 - Invalid Cursor State
    b'24': exceptions.InternalError,
    # Class 25 - Invalid Transaction State
    b'25': exceptions.InternalError,
    # Class 26 - Invalid SQL Statement Name
    b'26': exceptions.OperationalError,
    # Class 27 - Triggered Data Change Violation
    b'27': exceptions.OperationalError,
    # Class 28 - Invalid Authorization Specification
    b'28': exceptions.OperationalError,
    # Class 2B - Dependent Privilege Descriptors Still Exist
    b'2B': exceptions.InternalError,
    # Class 2D - Invalid Transaction Termination
    b'2D': exceptions.InternalError,
    # Class 2F - SQL Routine Exception
    b'2F': exceptions.InternalError,

    # Class 34 - Invalid Cursor Name
    b'34': exceptions.OperationalError,
    # Class 38 - External Routine Exception
    b'38': exceptions.InternalError,
    # Class 39 - External Routine Invocation Exception
    b'39': exceptions.InternalError,
    # Class 3B - Savepoint Exception
    b'3B': exceptions.InternalError,
    # Class 3D - Invalid Catalog Name
    b'3D': exceptions.ProgrammingError,
    # Class 3F - Invalid Schema Name
    b'3F': exceptions.ProgrammingError,

    # Class 40 - Transaction Rollback
    b'40': exceptions.ProgrammingError,  # TransactionRollbackError,
    # Class 42 - Syntax Error or Access Rule Violation
    b'42': exceptions.ProgrammingError,
    # Class 44 - WITH CHECK OPTION Violation
    b'44': exceptions.ProgrammingError,

    # b'55': exceptions.QueryCanceledError,
    # b'57': exceptions.QueryCanceledError,
    # b'50': exceptions.QueryCanceledError,
    # b'51': exceptions.QueryCanceledError,
    # b'54': exceptions.QueryCanceledError,
    # Class 53 - Insufficient Resources
    b'53': exceptions.OperationalError,
    # Class 54 - Program Limit Exceeded
    b'54': exceptions.OperationalError,
    # Class 55 - Object Not In Prerequisite State
    b'55': exceptions.OperationalError,
    # Class 57 - Operator Intervention
    b'57': exceptions.OperationalError,
    # Class 58 - System Error (errors external to PostgreSQL itself)
    b'58': exceptions.OperationalError,

    # Class F0 - Configuration File Error
    b'F0': exceptions.InternalError,
    # Class P0 - PL/pgSQL Error
    b'P0': exceptions.InternalError,
    # Class XX - Internal Error
    b'X': exceptions.InternalError,
}


class Connection(object):
    def __init__(self, PGconn, **kwargs):
        self.conn = PGconn
        self.kwargs = kwargs
        self.cursors = []
        self._status = libpq.PQstatus(PGconn)
        self._autocommit = False
        self.pid = libpq.PQbackendPID(PGconn)

    @property
    def _in_txn(self):
        txn_state = libpq.PQtransactionStatus(self.conn)
        return txn_state != libpq.PQTRANS_IDLE

    def close(self):
        '''
        Close the connection now (rather than whenever .__del__() is called).

        The connection will be unusable from this point forward; an Error (or
        subclass) exception will be raised if any operation is attempted with
        the connection. The same applies to all cursor objects trying to use
        the connection. Note that closing a connection without committing the
        changes first will cause an implicit rollback to be performed.
        '''
        while self.cursors:
            self.cursors[0].close()

        if self._in_txn:
            self.rollback()

        if self.conn is not None:
            libpq.PQfinish(self.conn)
            self.conn = None

    def commit(self):
        '''
        Commit any pending transaction to the database.

        Note that if the database supports an auto-commit feature, this must be
        initially off. An interface method may be provided to turn it back on.

        Database modules that do not support transactions should implement this
        method with void functionality.
        '''
        if self._in_txn:
            res = libpq.PQexec(self.conn, b'COMMIT')
            self._check_cmd_result(res)

    def rollback(self):
        '''
        This method is optional since not all databases provide transaction
        support.

        In case a database does provide transactions this method causes the
        database to roll back to the start of any pending transaction. Closing
        a connection without committing the changes first will cause an
        implicit rollback to be performed.
        '''
        if self._in_txn:
            res = libpq.PQexec(self.conn, b'ROLLBACK')
            self._check_cmd_result(res)

    def cursor(self):
        '''
        Return a new Cursor Object using the connection.

        If the database does not provide a direct cursor concept, the module
        will have to emulate cursors using other means to the extent needed by
        this specification.
        '''
        cursor = Cursor(self)
        self.cursors.append(cursor)
        return cursor

    def _close_cursor(self, cursor):
        '''
        Remove a cursor from out tracking list.
        '''
        try:
            self.cursors.remove(cursor)
        except ValueError:
            pass

    def _check_status(self):
        status = libpq.PQstatus(self.conn)
        if status == libpq.CONNECTION_OK:
            return
        msg = libpq.PQerrorMessage(self.conn)
        raise exceptions.DatabaseError(msg)
        # Raise appropriate error

    def _check_cmd_result(self, result):
        status = libpq.PQresultStatus(result)
        if status in (libpq.PGRES_COMMAND_OK, libpq.PGRES_TUPLES_OK):
            return self._check_status()
        msg = libpq.PQresultErrorMessage(result)
        msg = msg.decode('utf-8').strip()

        code = libpq.PQresultErrorField(result, libpq.PG_DIAG_SQLSTATE)
        if code:
            exc_class = EXC_MAP.get(code[:2], exceptions.DatabaseError)
        else:
            exc_class = exceptions.DatabaseError

        libpq.PQclear(result)
        raise exc_class(msg)
