
from collections import namedtuple
from ctypes import c_char_p

from .exceptions import *
from . import libpq
from . import types

Description = namedtuple('Description', (
    'name',
    'type_code',
    'display_size',
    'internal_size',
    'precision',
    'scale',
    'null_ok',
))


class Cursor(object):
    def __init__(self, conn):
        self.conn = conn
        self._result = None
        self._cleanup()

    def _cleanup(self):
        '''
        Internal function to clean up state when beginning a new operation.
        '''
        if self._result:
            libpq.PQclear(self._result)
            self._result = None
        self._rowcount = None
        self._description = None

    def _set_result(self, result):
        '''
        Given a result object, update all our attributes.
        '''
        self._cleanup()

        self._result = result
        self._nfields = nfields = libpq.PQnfields(result)
        desc = []
        for field in range(nfields):
            ftype = libpq.PQftype(result, field)
            fmod = libpq.PQfmod(result, field)
            fname = libpq.PQfname(result, field)
            fsize = libpq.PQfsize(result, field)
            desc.append(Description(
                fname,
                types.infer_type(ftype, fmod),
                None,
                fsize,
                None,
                None,
                None,
            ))
        self._description = desc
        self._rowcount = libpq.PQntuples(result)
        self._resultrow = -1

    def __iter__(self):
        '''
        Yield records from result.
        '''
        yield self.fetchone()

    def _prepare_param(self, param):
        if isinstance(param, bytes):
            return param
        if isinstance(param, str):
            return param.encode('utf-8')
        return str(param).encode('utf-8')

    @property
    def description(self):
        '''
        This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result
        column:

            name
            type_code
            display_size
            internal_size
            precision
            scale
            null_ok

        The first two items (name and type_code) are mandatory, the other five
        are optional and are set to None if no meaningful values can be
        provided.

        This attribute will be None for operations that do not return rows or
        if the cursor has not had an operation invoked via the .execute*()
        method yet.

        The type_code can be interpreted by comparing it to the Type Objects
        specified in the section below.
        '''
        return self._description

    @property
    def rowcount(self):
        '''
        This read-only attribute specifies the number of rows that the last
        .execute*() produced (for DQL statements like SELECT) or affected (for
        DML statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute*() has been performed on the
        cursor or the rowcount of the last operation is cannot be determined by
        the interface.
        '''
        return self._rowcount

    def callproc(self, procname, parameters=None):
        '''
        (This method is optional since not all databases provide stored
        procedures.)

        Call a stored database procedure with the given name. The sequence of
        parameters must contain one entry for each argument that the procedure
        expects. The result of the call is returned as modified copy of the
        input sequence. Input parameters are left untouched, output and
        input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then
        be made available through the standard .fetch*() methods.
        '''

    def close(self):
        '''
        Close the cursor now (rather than whenever __del__ is called).

        The cursor will be unusable from this point forward; an Error (or
        subclass) exception will be raised if any operation is attempted with
        the cursor.
        '''
        self._cleanup()
        self.conn._close_cursor(self)
        self.conn = None

    def execute(self, operation, parameters=None):
        '''
        Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to
        variables in the operation. Variables are specified in a
        database-specific notation (see the module's paramstyle attribute for
        details).

        A reference to the operation will be retained by the cursor. If the
        same operation object is passed in again, then the cursor can optimize
        its behavior. This is most effective for algorithms where the same
        operation is used, but different parameters are bound to it (many
        times).

        For maximum efficiency when reusing an operation, it is best to use the
        .setinputsizes() method to specify the parameter types and sizes ahead
        of time. It is legal for a parameter to not match the predefined
        information; the implementation should compensate, possibly with a loss
        of efficiency.

        The parameters may also be specified as list of tuples to e.g. insert
        multiple rows in a single operation, but this kind of usage is
        deprecated: .executemany() should be used instead.

        Return values are not defined.
        '''
        if isinstance(operation, str):
            operation = operation.encode('utf-8')

        if parameters is None:
            parameters = []

        parameters = [
            self._prepare_param(param)
            for param in parameters
        ]
        param_array = c_char_p * len(parameters)
        params = param_array(*parameters)

        result = libpq.PQexecParams(self.conn.conn, operation, len(parameters), None, params, None, None, 1)

        # Did it succeed?
        status = libpq.PQresultStatus(result)
        if status == libpq.PGRES_FATAL_ERROR:
            msg = libpq.PQresultErrorMessage(result)
            libpq.PQclear(result)
            raise SyntaxError(msg)

        self._set_result(result)

    def executemany(self, operation, seq_of_parameters):
        '''
        Prepare a database operation (query or command) and then execute it
        against all parameter sequences or mappings found in the sequence
        seq_of_parameters.

        Modules are free to implement this method using multiple calls to the
        .execute() method or by using array operations to have the database
        process the sequence as a whole in one call.

        Use of this method for an operation which produces one or more result
        sets constitutes undefined behavior, and the implementation is
        permitted (but not required) to raise an exception when it detects that
        a result set has been created by an invocation of the operation.

        The same comments as for .execute() also apply accordingly to this
        method.

        Return values are not defined.
        '''
        # Prepare
        prepared = libpq.PQprepare(self.conn, '', operation, len(seq_of_parameters[0]), self._guess_types(seq_of_parameters[0]))
        #
        result = None
        for params in seq_of_parameters:
            if result:
                libpq.PQclear(result)
            result = self.executeprepared(prepared, params)

        # Need a hook for this
        self._set_result(result)

    def fetchone(self):
        '''
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        '''
        if not self._result:
            raise InterfaceError('No results to fetch.')

        if self._resultrow == self._rowcount:
            return None
        self._resultrow += 1
        rownum = self._resultrow

        rec = []
        for idx, desc in enumerate(self._description):
            val = libpq.PQgetvalue(self._result, rownum, idx)
            vlen = libpq.PQgetlength(self._result, rownum, idx)
            if not val and libpq.PQgetisnull(self._result, rownum, idx):
                val = None
            else:
                val = desc.type_code(val, vlen)
            rec.append(val)
        return rec

    def fetchmany(size=None):
        # size = cursor.arraysize
        '''
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If
        it is not given, the cursor's arraysize determines the number of rows
        to be fetched. The method should try to fetch as many rows as indicated
        by the size parameter. If this is not possible due to the specified
        number of rows not being available, fewer rows may be returned.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter. For optimal performance, it is usually best to use the
        .arraysize attribute. If the size parameter is used, then it is best
        for it to retain the same value from one .fetchmany() call to the next.
        '''

    def fetchall(self):
        '''
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        '''

    def nextset(self):
        '''
        (This method is optional since not all databases support multiple
        result sets.)

        This method will make the cursor skip to the next available set,
        discarding any remaining rows from the current set.

        If there are no more sets, the method returns None. Otherwise, it
        returns a true value and subsequent calls to the .fetch*() methods will
        return rows from the next result set.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        '''

    def _get_arraysize(self):
        '''
        This read/write attribute specifies the number of rows to fetch at a
        time with .fetchmany(). It defaults to 1 meaning to fetch a single row
        at a time.

        Implementations must observe this value with respect to the
        .fetchmany() method, but are free to interact with the database a
        single row at a time. It may also be used in the implementation of
        .executemany().
        '''

    def _set_arraysize(self):
        pass

    arraysize = property(_get_arraysize, _set_arraysize)

    def setinputsizes(self, sizes):
        '''
        This can be used before a call to .execute*() to predefine memory areas
        for the operation's parameters.

        sizes is specified as a sequence - one item for each input parameter.
        The item should be a Type Object that corresponds to the input that
        will be used, or it should be an integer specifying the maximum length
        of a string parameter. If the item is None, then no predefined memory
        area will be reserved for that column (this is useful to avoid
        predefined areas for large inputs).

        This method would be used before the .execute*() method is invoked.

        Implementations are free to have this method do nothing and users are
        free to not use it.
        '''

    def setoutputsize(self, size, column=None):
        '''
        Set a column buffer size for fetches of large columns (e.g. LONGs,
        BLOBs, etc.). The column is specified as an index into the result
        sequence. Not specifying the column will set the default size for all
        large columns in the cursor.

        This method would be used before the .execute*() method is invoked.

        Implementations are free to have this method do nothing and users are
        free to not use it.
        '''
