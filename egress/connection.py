
from . import libpq

class Connection(object):
    def __init__(self, PGconn, **kwargs):
        self.conn = PGconn
        self.kwargs = kwargs
        self.cursors = []

    def close(self):
        '''
        Close the connection now (rather than whenever .__del__() is called).

        The connection will be unusable from this point forward; an Error (or
        subclass) exception will be raised if any operation is attempted with
        the connection. The same applies to all cursor objects trying to use
        the connection. Note that closing a connection without committing the
        changes first will cause an implicit rollback to be performed.
        '''
        for cursor in self.cursors:
            cursor.close()

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
        res = libpq.PQexec(self.conn, 'COMMIT')
        status = libpq.PQresultStatus(res)


    def rollback(self):
        '''
        This method is optional since not all databases provide transaction
        support.

        In case a database does provide transactions this method causes the
        database to roll back to the start of any pending transaction. Closing
        a connection without committing the changes first will cause an implicit
        rollback to be performed.
        '''
        libpq.PQexec(self.conn, 'ROLLBACK')

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
