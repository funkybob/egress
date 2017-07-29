from ctypes import cast, c_char_p
import datetime
from functools import partial
from ipaddress import IPv4Address, IPv6Address, IPv4Network, IPv6Network
import json
import struct

'''
The module exports the following constructors and singletons:
'''
INTEGER_DATETIMES = False


def Date(year, month, day):
    '''
    This function constructs an object holding a date value.
    '''


def Time(hour, minute, second):
    '''
    This function constructs an object holding a time value.
    '''


def Timestamp(year, month, day, hour, minute, second):
    '''
    This function constructs an object holding a time stamp value.
    '''


def DateFromTicks(ticks):
    '''
    This function constructs an object holding a date value from the given
    ticks value (number of seconds since the epoch; see the documentation of
    the standard Python time module for details).
    '''


def TimeFromTicks(ticks):
    '''
    This function constructs an object holding a time value from the given
    ticks value (number of seconds since the epoch; see the documentation of
    the standard Python time module for details).
    '''


def TimestampFromTicks(ticks):
    '''
    This function constructs an object holding a time stamp value from the
    given ticks value (number of seconds since the epoch; see the documentation
    of the standard Python time module for details).
    '''


def Binary(string):
    '''
    This function constructs an object capable of holding a binary (long)
    string value.
    '''


class DBAPITypeObject(object):
    '''Copied from PEP-249'''
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


# This type object is used to describe columns in a database that are
# string-based (e.g. CHAR).
STRING = DBAPITypeObject()

# This type object is used to describe (long) binary columns in a database
# (e.g. LONG, RAW, BLOBs).
BINARY = DBAPITypeObject()

# This type object is used to describe numeric columns in a database.
NUMBER = DBAPITypeObject()

# This type object is used to describe date/time columns in a database.
DATETIME = DBAPITypeObject()

# This type object is used to describe the "Row ID" column in a database.
ROWID = DBAPITypeObject()


# Type conversion functions:
def parse_bool(value, vlen, ftype=None, fmod=None):
    return value[0] == '\x01'


def parse_bytea(value, vlen, ftype=None, fmod=None):
    return value[:vlen]


def parse_char(value, vlen, ftype=None, fmod=None):
    return value[0]


def parse_integer(value, vlen, ftype=None, fmod=None):
    if vlen == -1:
        return None
    if vlen == 0:
        return 0
    if vlen == 2:
        return struct.unpack('!h', value[:vlen])[0]
    if vlen == 4:
        return struct.unpack('!i', value[:vlen])[0]
    if vlen == 8:
        return struct.unpack("!q", value[:vlen])[0]
    raise ValueError('Unexpected length for INT type: %r' % vlen)


def parse_timestamp(value, vlen, ftype=None, fmod=None):
    assert vlen == 8, 'Invalid timestamp len: %d (%r)' % (vlen, value[:vlen])
    if INTEGER_DATETIMES:
        # data is 64-bit integer representing milliseconds since 2000-01-01
        val = struct.unpack('!q', value[:vlen])[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(microseconds=val)
    else:
        # data is double-precision float representing seconds since 2000-01-01
        val = struct.unpack('!d', value[:vlen])[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=val)


def parse_string(value, vlen, ftype=None, fmod=None):
    return cast(value, c_char_p).value


def parse_dummy(value, vlen, ftype=None, fmod=None):
    print("Dummy: %r %r" % (value, vlen))
    return value


def parse_ipaddr(value, vlen, ftype=None, fmod=None):
    ip_family, ip_bits, is_cidr, nb = struct.unpack('BBBB', value[:4])
    if nb == 4:
        if ip_bits:
            return IPv4Network((value[4:4+nb], ip_bits))
        return IPv4Address(value[4:4+nb])
    elif nb == 16:
        if ip_bits:
            return IPv6Network((value[4:4+nb], ip_bits))
        return IPv6Address(value[4:4+nb])
    return value


def parse_jsonb(value, vlen, ftype=None, fmod=None):
    if value[0] == b'\x01':
        return json.loads(value[1:vlen].decode('utf-8'))
    return value[:vlen]


def parse_float(value, vlen, ftype=None, fmod=None):
    return struct.unpack('!f', value[:vlen])[0]


def parse_double(value, vlen, ftype=None, fmod=None):
    return struct.unpack('!d', value[:vlen])[0]


# DESCR() strings taken from pg_type.h
TYPE_MAP = {
    # DESCR("boolean, 'true'/'false'")
    16: parse_bool,
    # DESCR("variable-length string, binary values escaped")
    17: parse_bytea,
    # DESCR("single character");
    18: parse_char,
    # DESCR("63-byte type for storing system identifiers")
    # 19:
    # DESCR("-2 billion to 2 billion integer, 4-byte storage")
    23: parse_integer,
    # DESCR("variable-length string, no limit specified")
    25: parse_string,
    26: parse_integer,
    # DESCR("network IP address/netmask, network address")
    650: parse_ipaddr,
    # DESCR("single-precision floating point number, 4-byte storage")
    701: parse_float,
    # DESCR("double-precision floating point number, 8-byte storage")
    701: parse_double,
    # DESCR("IP address/netmask, host address, netmask optional")
    869: parse_ipaddr,
    # "varchar(length), non-blank-padded string, variable storage length"
    1043: parse_string,
    1114: parse_timestamp,
    # DESCR("Binary JSON")
    3802: parse_jsonb,
}


def infer_type(ftype, fmod):
    '''
    Given a postgres type OID and modifier, infer the related Type class
    '''
    if ftype not in TYPE_MAP:
        print("Unknown type: %r:%r" % (ftype, fmod))
    return partial(
        TYPE_MAP.get(ftype, parse_dummy),
        ftype=ftype,
        fmod=fmod,
    )
