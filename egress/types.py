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


def infer_parser(ftype, fmod):
    '''
    Given a postgres type OID and modifier, infer the related Type class
    '''
    return BaseType._oid[ftype](ftype, fmod)


def format_type(value):
    try:
        return BaseType._type[type(value)](value) + (1,)
    except KeyError:
        value = str(value).encode('utf-8')
        return (0, value, 0, 0)


class BaseTypeMeta(type):
    def __new__(cls, name, bases, namespace, **kwds):
        new_cls = super().__new__(cls, name, bases, namespace, **kwds)
        if new_cls.oid is not None:
            new_cls._oid[new_cls.oid] = new_cls
        if new_cls.klass is not None:
            new_cls._type[new_cls.klass] = new_cls
        return new_cls


class BaseType(metaclass=BaseTypeMeta):
    _oid = {}
    _type = {}

    oid = None
    klass = None
    fmt = ''

    def __init__(self, ftype, fmod):
        self.size = struct.calcsize(self.fmt)
        self.ftype = ftype
        self.fmod = fmod

    def parse(self, value, size):
        assert size == self.size
        return struct.unpack(self.fmt, value[:size])[0]

    @staticmethod
    def format(value):
        return (self.oid, struct.pack(self.fmt, value), self.size)


class NoneType(BaseType):
    klass = type(None)

    @staticmethod
    def format(value):
        return (0, None, 0,)


class BoolType(BaseType):
    fmt = '?'

    oid = 16
    klass = bool


class BinaryType(BaseType):
    oid = 17
    klass = bytes

    def parse(self, value, size):
        self.size = size
        return self.value[:size]

    @staticmethod
    def format(value):
        return (17, c_char_p(value), len(value))


class CharType(BaseType):
    oid = 18
    fmt = 'c'

    def parse(self, value, size):
        self.size = size
        return value[:size].decode('utf-8')



class ShortIntType(BaseType):
    oid = 21
    fmt = '!h'

class IntType(BaseType):
    oid = 23
    fmt = '!i'


class LongType(BaseType):
    oid = 20
    fmt = '!q'
    klass = int

    @staticmethod
    def format(value):
        bits = value.bit_length()
        if bits < 16:
            return (21, struct.pack('!h', value), 2)
        elif bits < 32:
            return (23, struct.pack('!i', value), 4)
        else:
            return (20, struct.pack('!q', value), 8)


class OidType(BaseType):
    oid = 26
    fmt = '!q'


class DateType(BaseType):
    oid = 1082
    klass = datetime.date
    fmt = '!i'

    def parse(self, value, size):
        val = struct.unpack(self.fmt, value[:size])[0]
        return datetime.date(2000, 1, 1) + datetime.timedelta(days=val)

    @staticmethod
    def format(value):
        val = (value - datetime.date(2000, 1, 1)).days
        return (1082, struct.pack('!i', val), struct.calcsize('!i'))


class TimestampTzType(BaseType):
    oid = 1184
    klass = datetime.datetime
    fmt = '!q'

    def parse(self, value, size):
        val = struct.unpack(self.fmt, value[:size])[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(microseconds=val)

    def format(value):
        if value.tzinfo:
            val = (value - datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))
        else:
            val = (value - datetime.datetime(2000, 1, 1))
        val = int(val.total_seconds() * 1000000)
        return (1184, struct.pack(self.fmt, val), self.size)



class StringType(BaseType):
    oid = 25

    def parse(self, value, size):
        self.size = size
        return value[:size].decode('utf-8')


class BlankPaddedString(StringType):
    '''
    char(length), blank-padded string, fixed storage length
    '''
    oid = 1042


class VarCharType(StringType):
    oid = 1043


# Django often passes strings when it means other types, and assumes the SQL
# parsing will figure it out :/
# @register_format(str)
# def format_string(value):
#     value = value.encode('utf-8')
#     length = len(value)
#     return (1042, struct.pack('%ds' % length, value), length,)


class IPv4AddressType(BaseType):
    '''
    network IP address/netmask, network address
    '''
    oid = 650
    klass = IPv4Address

    def parse(self, value, size):
        self.size = size
        ip_family, ip_bits, is_cidr, nb = struct.unpack('BBBB', value[:4])
        if nb == 4:
            if ip_bits:
                return IPv4Network((value[4:4+nb], ip_bits))
            return IPv4Address(value[4:4+nb])
        elif nb == 16:
            if ip_bits:
                return IPv6Network((value[4:4+nb], ip_bits))
            return IPv6Address(value[4:4+nb])


class IPv4NetworkType(IPv4AddressType):
    oid = None
    klass = IPv4Network


class IPv6AddressType(IPv4AddressType):
    oid = None
    klass = IPv6Address


class IPv6NetworkType(IPv4AddressType):
    oid = None
    klass = IPv6Network


class IPAddressType(IPv4AddressType):
    oid = 869


class JsonbType(BaseType):
    oid = 3802

    def parse(self, value, size):
        self.size = size
        if value[0] == b'\x01':
            return json.loads(value[1:vlen].decode('utf-8'))
        return value[:vlen].decode('utf-8')


class FloatType(BaseType):
    oid = 700
    fmt = '!f'


class DoubleType(BaseType):
    oid = 701
    klass = float
    fmt = '!d'


class NameDataType(BaseType):
    oid = 19

    def parse(self, value, size):
        return value[:size].decode('utf-8')


class UUIDType(BaseType):
    oid = 2950
    klass = uuid.UUID

    def parse(self, value, size):
        self.size = size
        return uuid.UUID(bytes=value[:vlen])

    @staticmethod
    def format(value):
        return (self.oid, value.bytes, 16)


class NumericType(BaseType):
    oid = 1700
    klass = Decimal

    def parse(self, value, size):
        self.size = size
        hsize = struct.calcsize('!HhHH')
        ndigits, weight, sign, dscale = struct.unpack('!HhHH', value[:hsize])
        if sign == 0xc000:
            return Decimal('NaN')
        desc = '!%dH' % ndigits
        digits = struct.unpack(desc, value[hsize:hsize+struct.calcsize(desc)])
        n = '-' if sign else ''
        # numeric has a form of compression where if the remaining digits are 0,
        # they are not sent, even if we haven't reached the decimal point yet!
        while weight >= len(digits):
            digits = digits + (0,)

        for idx, digit in enumerate(digits):
            n += '{:04d}'.format(digit)
            if idx == weight:
                n += '.'
        n = Decimal(n)
        return n

    @staticmethod
    def format(value):
        sign, digits, exponent = value.as_tuple()
        dscale = abs(exponent)
        if exponent:
            frac = digits[exponent:]
            digits = digits[:exponent]
        else:
            frac = []
        vals = []
        while digits:
            vals.append(int(''.join(map(str, digits[:4]))))
            digits = digits[4:]
        weight = len(vals)
        while frac:
            d = (frac[:4] + ('0',) * 4)[:4]
            frac = frac[4:]
            vals.append(int(''.join(map(str, d))))
        fmt = '!HhHH%dH' % len(vals)

        val = struct.pack(fmt , len(vals), max(0, weight-1), 0xc000 if sign else 0, dscale, *vals)
        return (self.oid, val, struct.calcsize(fmt))


class IntervalType(BaseType):
    oid = 1186
    klass = datetime.timedelta
    fmt = '!qii'

    def parse(value, size):
        time_us, days, months = struct.unpack(self.fmt, value[:size])
        val = datetime.timedelta(days=days + months * 30, microseconds=time_us)
        return val

    @staticmethod
    def format(value):
        months, days = divmod(value.days, 30)
        usec = value.seconds * 1000000 + value.microseconds
        return (self.oid, struct.pack(self.fmt, usec, days, months), self.size)


class TimeOfDayType(BaseType):
    '''
    64bit int of uSec since midnight.
    '''
    oid = 1083
    klass = datetime.time
    fmt = '!q'

    def parse(self, value, vlen):
        time_us = struct.unpack(self.fmt, value[:size])[0]
        val, microsecond = divmod(time_us, 1000000)
        val, second = divmod(val, 60)
        hour, minute = divmod(val, 60)
        return datetime.time(hour, minute, second, microsecond)

    @staticmethod
    def format(value):
        val = ((value.hour * 60 + value.minute) * 60 + value.second) * 1000000 + value.microsecond
        return (self.oid, struct.pack(self.fmt, val), self.size)


class UnknownType(BaseType):
    oid = 705

    def parse(self, value, vlen):
        self.size = size
        return value[:size].decode('utf-8')
