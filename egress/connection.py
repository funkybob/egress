import logging

from . import libpq, exceptions
from .cursor import Cursor


log = logging.getLogger(__name__)


EXC_MAP = {
    # Class 0A - Feature Not Supported
    '0A': exceptions.NotSupportedError,

    # Class 21 - Cardinality Violation
    '21': exceptions.ProgrammingError,
    # Class 22 - Data Exception
    '22': exceptions.DataError,
    # Class 23 - Integrity Constraint Violation
    '23': exceptions.IntegrityError,
    # Class 24 - Invalid Cursor State
    '24': exceptions.InternalError,
    # Class 25 - Invalid Transaction State
    '25': exceptions.InternalError,
    # Class 26 - Invalid SQL Statement Name
    '26': exceptions.OperationalError,
    # Class 27 - Triggered Data Change Violation
    '27': exceptions.OperationalError,
    # Class 28 - Invalid Authorization Specification
    '28': exceptions.OperationalError,
    # Class 2B - Dependent Privilege Descriptors Still Exist
    '2B': exceptions.InternalError,
    # Class 2D - Invalid Transaction Termination
    '2D': exceptions.InternalError,
    # Class 2F - SQL Routine Exception
    '2F': exceptions.InternalError,

    # Class 34 - Invalid Cursor Name
    '34': exceptions.OperationalError,
    # Class 38 - External Routine Exception
    '38': exceptions.InternalError,
    # Class 39 - External Routine Invocation Exception
    '39': exceptions.InternalError,
    # Class 3B - Savepoint Exception
    '3B': exceptions.InternalError,
    # Class 3D - Invalid Catalog Name
    '3D': exceptions.ProgrammingError,
    # Class 3F - Invalid Schema Name
    '3F': exceptions.ProgrammingError,

    # Class 40 - Transaction Rollback
    '40': exceptions.ProgrammingError,  # TransactionRollbackError,
    # Class 42 - Syntax Error or Access Rule Violation
    '42': exceptions.ProgrammingError,
    # Class 44 - WITH CHECK OPTION Violation
    '44': exceptions.ProgrammingError,

    # '55': exceptions.QueryCanceledError,
    # '57': exceptions.QueryCanceledError,
    # '50': exceptions.QueryCanceledError,
    # '51': exceptions.QueryCanceledError,
    # '54': exceptions.QueryCanceledError,
    # Class 53 - Insufficient Resources
    '53': exceptions.OperationalError,
    # Class 54 - Program Limit Exceeded
    '54': exceptions.OperationalError,
    # Class 55 - Object Not In Prerequisite State
    '55': exceptions.OperationalError,
    # Class 57 - Operator Intervention
    '57': exceptions.OperationalError,
    # Class 58 - System Error (errors external to PostgreSQL itself)
    '58': exceptions.OperationalError,

    # Class F0 - Configuration File Error
    'F0': exceptions.InternalError,
    # Class P0 - PL/pgSQL Error
    'P0': exceptions.InternalError,
    # Class XX - Internal Error
    'X': exceptions.InternalError,
}


def requires_open(func):
    def _wrapper(self, *args, **kwargs):
        if not self.conn:
            raise exceptions.Error('No connection')
        return func(self, *args, **kwargs)
    return _wrapper


class Connection(object):
    def __init__(self, conn, **kwargs):
        self.conn = conn
        self.kwargs = kwargs
        self.cursors = []
        self._status = self.conn.status()
        self._autocommit = False
        self.pid = self.conn.pid()
        self.tzinfo = None

    @property
    @requires_open
    def _in_txn(self):
        txn_state = self.conn.transaction_status()
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
            self.conn.finish()
            self.conn = None

    @requires_open
    def commit(self):
        '''
        Commit any pending transaction to the database.

        Note that if the database supports an auto-commit feature, this must be
        initially off. An interface method may be provided to turn it back on.

        Database modules that do not support transactions should implement this
        method with void functionality.
        '''
        if self._in_txn:
            res = self.conn.execute('COMMIT')
            self._check_cmd_result(res)

    @requires_open
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
            res = self.conn.execute('ROLLBACK')
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

    @requires_open
    def _check_conn_status(self):
        status = self.conn.status()
        if status == libpq.CONNECTION_OK:
            return
        msg = self.conn.error_message()
        raise exceptions.DatabaseError(msg)

    def _check_cmd_result(self, result):
        status = result.status()
        if status in (libpq.PGRES_COMMAND_OK, libpq.PGRES_TUPLES_OK):
            return self._check_conn_status()

        msg = result.error_message()
        if status == libpq.PGRES_NONFATAL_ERROR:
            log.warning(msg)
            return self._check_conn_status()

        code = result.error_field(libpq.PG_DIAG_SQLSTATE)
        if code:
            exc_class = EXC_MAP.get(code[:2], exceptions.DatabaseError)
        else:
            exc_class = exceptions.DatabaseError

        result.clear()
        raise exc_class(msg)
