import itertools
import re

from collections import namedtuple
from ctypes import c_char_p, c_int, c_uint

from . import libpq, types
from .exceptions import InterfaceError


PARAM_RE = re.compile('\$(\d+)')

Description = namedtuple('Description', (
    'name',
    'type_code',
    'display_size',
    'internal_size',
    'precision',
    'scale',
    'null_ok',
    'cast_func',
))


class Cursor(object):
    def __init__(self, conn):
        self.conn = conn
        self.query = None
        self.arraysize = 1
        self._result = None
        self.tzinfo = self.conn.tzinfo
        self._cleanup()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _free_result(self):
        if self._result:
            self._result.clear()
            self._result = None

    def _cleanup(self):
        '''
        Internal function to clean up state when beginning a new operation.
        '''
        self._free_result()
        self._rowcount = None
        self._description = None

    def _set_result(self, result):
        '''
        Given a result object, update all our attributes.
        '''
        self._cleanup()

        self._result = result
        self._nfields = nfields = result.nfields()

        status = result.status()
        if status == libpq.PGRES_COMMAND_OK:
            count = result.cmd_tuples()
            if count:
                self._rowcount = int(count)
            else:
                self._rowcount = -1
        elif status == libpq.PGRES_TUPLES_OK:
            self._rowcount = result.ntuples()

        desc = []
        for field in range(nfields):
            ftype = result.field_type(field)
            fmod = result.field_modifier(field)
            fname = result.field_name(field)
            fsize = result.field_size(field)
            if fmod > 0:
                fmod -= 4
            if fsize == -1:
                if ftype == 1700:   # Numeric
                    isize = fmod >> 16
                else:
                    isize = fmod
            else:
                isize = fsize

            if ftype == 1700:
                prec = (fmod >> 16) & 0xFFFF
                scale = fmod & 0xFFFF
            else:
                prec = scale = None

            try:
                cast_func = types.infer_parser(ftype, fmod)
            except:
                raise TypeError('Unknown type for field %r: %r %r' % (fname, ftype, fmod))
            desc.append(Description(
                fname,
                ftype,
                None,
                isize,
                prec,
                scale,
                None,
                cast_func,
            ))
        self._description = desc
        self._resultrow = -1

    def __iter__(self):
        return self

    def __next__(self):
        '''
        Yield records from result.
        '''
        row = self.fetchone()
        if row is None:
            raise StopIteration()
        return row

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
        cursor or the rowcount of the last operation cannot be determined by
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
        raise NotImplementedError

    def close(self):
        '''
        Close the cursor now (rather than whenever __del__ is called).

        The cursor will be unusable from this point forward; an Error (or
        subclass) exception will be raised if any operation is attempted with
        the cursor.
        '''
        self._cleanup()
        if self.conn:
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
        # Convert %s -> $n
        ctr = itertools.count(1)
        def repl(match):
            return '$%d' % next(ctr)
        operation = re.sub('(?<!%)%s', repl, operation)
        operation = operation.replace('%%', '%')

        param_count = len(PARAM_RE.findall(operation))

        if isinstance(operation, str):
            operation = operation.encode('utf-8')

        if parameters:
            pcount = len(parameters)
            if pcount != param_count:
                raise InterfaceError('Incorrect number of parameters: %d (exptedted %d)' % (
                    pcount, param_count,
                ))

            paramTypes = (c_uint * pcount)()
            paramValues = (c_char_p * pcount)()
            paramLengths = (c_int * pcount)()
            paramFormats = (c_int * pcount)()
            for idx, param in enumerate(parameters):
                t, v, l, f = types.format_type(param)
                paramTypes[idx] = t
                paramValues[idx] = v
                paramLengths[idx] = l
                paramFormats[idx] = f

        else:
            parameters = []
            paramTypes = paramValues = paramLengths = paramFormats = None

        self.query = PARAM_RE.sub(
            lambda m: str(parameters[int(m.group(0).lstrip('$'))-1]),
            operation.decode('utf-8')
        )

        # print('{%r:%r}[A:%r T:%r] %r : %r' % (id(self.conn), id(self), self.conn._autocommit, self.conn._in_txn, operation, parameters))
        if not (self.conn._autocommit or self.conn._in_txn):
            result = self.conn.conn.execute('BEGIN')
            self.conn._check_cmd_result(result)
        result = self.conn.conn.exec_params(
            operation,
            len(parameters),
            paramTypes,
            paramValues,
            paramLengths,
            paramFormats,
            1
        )
        # print(result.cmd_status())

        # Did it succeed?
        self.conn._check_cmd_result(result)

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
        result = None
        for params in seq_of_parameters:
            if result:
                result.clear()
            result = self.execute(operation, params)

        # Need a hook for this
        if result is not None:
            self._set_result(result)

    def fetchone(self):
        '''
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        '''
        if not self._result:
            return None

        self._resultrow += 1
        if self._resultrow >= self._rowcount:
            self._free_result()
            return None
        rownum = self._resultrow

        tzinfo = self.tzinfo

        rec = []
        for idx, desc in enumerate(self._description):
            val = self._result.get_value(rownum, idx)
            vlen = self._result.get_length(rownum, idx)
            if not val and self._result.get_isnull(rownum, idx):
                val = None
            else:
                val = desc.cast_func.parse(val, vlen, tzinfo)
            rec.append(val)
        return tuple(rec)

    def fetchmany(self, size=None):
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
        if size is None:
            size = self.arraysize
        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def fetchall(self):
        '''
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        '''
        return [row for row in self]
        result = []
        row = self.fetchone()
        while(row):
            result.append(row)
            row = self.fetchone()
        return result

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
        raise NotImplementedError

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
        raise NotImplementedError

    def _set_arraysize(self):
        raise NotImplementedError

    # arraysize = property(_get_arraysize, _set_arraysize)

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
        raise NotImplementedError

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
        raise NotImplementedError
