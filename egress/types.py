from ctypes import cast, c_char_p, c_int

import datetime
import json
import struct
import uuid

from decimal import Decimal
from functools import partial
from ipaddress import IPv4Address, IPv6Address, IPv4Network, IPv6Network

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


PARSER_MAP = {}
FORMAT_MAP = {}


def register_format(typ):
    def _inner(func):
        FORMAT_MAP[typ] = func
        return func
    return _inner


def register_parser(oid):
    def _inner(func):
        PARSER_MAP[oid] = func
        return func
    return _inner


@register_format(type(None))
def format_none(value):
    return (0, None, 0,)


@register_parser(16)
def parse_bool(value, vlen, ftype=None, fmod=None):
    return struct.unpack('?', value[:1])[0]


@register_format(bool)
def format_bool(value):
    return (16, struct.pack('?', value), 1)


@register_parser(17)
def parse_bytea(value, vlen, ftype=None, fmod=None):
    return value[:vlen]


@register_format(bytes)
def format_bytea(value):
    return (17, c_char_p(value), len(value))


@register_parser(18)
def parse_char(value, vlen, ftype=None, fmod=None):
    return value[:1].decode('utf-8')


@register_parser(21)
@register_parser(23)
@register_parser(26)
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


@register_parser(20)
def parse_uint(value, vlen, ftype=None, fmod=None):
    if vlen == 0:
        return 0
    if vlen == 2:
        return struct.unpack('!H', value[:vlen])[0]
    if vlen == 4:
        return struct.unpack('!I', value[:vlen])[0]
    if vlen == 8:
        return struct.unpack("!Q", value[:vlen])[0]
    raise ValueError('Unexpected length for INT type: %r' % vlen)


@register_parser(1082)
def parse_date(value, vlen, ftype=None, fmod=None):
    val = struct.unpack('!i', value[:vlen])[0]
    return (datetime.datetime(2000, 1, 1) + datetime.timedelta(days=val)).date()


@register_parser(1114)
def parse_timestamp(value, vlen, ftype=None, fmod=None):
    val = struct.unpack('!d', value[:vlen])[0]
    return datetime.datetime(2000, 1, 1) + datetime.timedelta(microseconds=val)


@register_parser(1184)
def parse_timestamp_tz(value, vlen, ftype=None, fmod=None):
    val = struct.unpack('!d', value[:vlen])[0]
    return datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=val)


@register_format(datetime)
def format_timestamp(value):
    value = (value - datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))
    value = int(value.total_seconds() * 1000000)
    return (1184, struct.pack('!d', value), 8)


@register_parser(25)
@register_parser(1042)
@register_parser(1043)
def parse_string(value, vlen, ftype=None, fmod=None):
    return cast(value, c_char_p).value.decode('utf-8')


@register_format(str)
def format_string(value):
    value = value.encode('utf-8')
    length = len(value)
    return (1042, struct.pack('%ds' % length, value), length,)


@register_parser(650)
@register_parser(869)
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


@register_parser(3802)
def parse_jsonb(value, vlen, ftype=None, fmod=None):
    if value[0] == b'\x01':
        return json.loads(value[1:vlen].decode('utf-8'))
    return value[:vlen].decode('utf-8')


@register_parser(700)
def parse_float(value, vlen, ftype=None, fmod=None):
    return struct.unpack('!f', value[:vlen])[0]


@register_parser(701)
def parse_double(value, vlen, ftype=None, fmod=None):
    return struct.unpack('!d', value[:vlen])[0]


@register_parser(float)
def format_double(value):
    return (701, struct.pack('!d', value), struct.calcsize('!d'))


@register_parser(19)
def parse_namedata(value, vlen, ftype=None, fmod=None):
    return value[:vlen].decode('utf-8')


@register_parser(2950)
def parse_uuid(value, vlen, ftype=None, fmod=None):
    return uuid.uuid(bytes=value[:vlen])


@register_parser(1700)
def parse_numeric(value, vlen, ftype=None, fmod=None):
    hsize = struct.calcsize('!hhhh')
    ndigits, weight, sign, dscale = struct.unpack('!hhhh', value[:hsize])
    desc = '!%dh' % ndigits
    digits = struct.unpack(desv, value[hsize:hsize+struct.calcsize(desc)])
    return Decimal('0')


# @register_format(Decimal)
def format_decimal(value):
    return (1700, value, 1)


def infer_parser(ftype, fmod):
    '''
    Given a postgres type OID and modifier, infer the related Type class
    '''
    if ftype not in PARSER_MAP:
        raise KeyError("Unknown type: %r:%r" % (ftype, fmod))
    return partial(
        PARSER_MAP[ftype],
        ftype=ftype,
        fmod=fmod,
    )


def format_type(value):
    if isinstance(value, Decimal):
        value = float(value)
    try:
        return FORMAT_MAP[type(value)](value) + (1,)
    except KeyError:
        value = str(value).encode('utf-8')
        return (0, value, 0, 0)
