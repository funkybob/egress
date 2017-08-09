from ctypes import cast, c_char_p, c_int

import datetime
import json
import struct
import uuid

from decimal import Decimal
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


def infer_parser(ftype, fmod=-1):
    '''
    Given a postgres type OID and modifier, infer the related Type class
    '''
    return BaseType._oid[ftype]


def format_type(value):
    try:
        return BaseType._type[type(value)].format(value) + (1,)
    except KeyError:
        value = str(value).encode('utf-8')
        return (0, value, 0, 0)


class BaseTypeMeta(type):
    def __new__(cls, name, bases, namespace, **kwds):
        if 'fmt' in namespace and 'size' not in namespace:
            namespace['size'] = struct.calcsize(namespace['fmt'])
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

    @classmethod
    def parse(cls, value, size):
        if size == 0:
            return None
        return struct.unpack(cls.fmt, value[:size])[0]

    @classmethod
    def format(cls, value):
        return (cls.oid, struct.pack(cls.fmt, value), cls.size)


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

    @staticmethod
    def parse(value, size):
        return value[:size]

    @staticmethod
    def format(value):
        return (17, c_char_p(value), len(value))


class CharType(BaseType):
    oid = 18
    fmt = 'c'

    @staticmethod
    def parse(value, size):
        return value[:size].decode('utf-8')


class NameDataType(BaseType):
    oid = 19

    @staticmethod
    def parse(value, size):
        if size == 0:
            return None
        return value[:size].decode('utf-8')


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


class ShortIntType(BaseType):
    oid = 21
    fmt = '!h'

class IntType(BaseType):
    oid = 23
    fmt = '!i'


class StringType(BaseType):
    '''
    Django often passes strings when it means other types, and assumes the SQL
    parsing will figure it out :/
    To deal with this, we don't define a formatter, and pass all string
    arguments as "guess this" text.
    '''
    oid = 25

    @staticmethod
    def parse(value, size):
        if not size:
            return ''
        return value[:size].decode('utf-8')


class OidType(BaseType):
    oid = 26
    fmt = '!q'


class IPv4AddressType(BaseType):
    '''
    network IP address/netmask, network address
    '''
    oid = 650
    klass = IPv4Address

    @staticmethod
    def parse(value, size):
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


class FloatType(BaseType):
    oid = 700
    fmt = '!f'


class DoubleType(BaseType):
    oid = 701
    klass = float
    fmt = '!d'


class UnknownType(BaseType):
    oid = 705

    @staticmethod
    def parse(value, size):
        if not size:
            return None
        return value[:size].decode('utf-8')


class IPAddressType(IPv4AddressType):
    oid = 869


class ArrayType(BaseType):

    @staticmethod
    def parse(value, size):
        if not size:
            return []
        ndim, flags, element_type = struct.unpack('!iii', value[:12])

        dim_info = []
        offs = 12
        for dim in range(ndim):
            dim_info.append(
                struct.unpack('!ii', value[offs:offs+8])
            )
            offs += 8

        assert ndim == 1, 'Only single dimension arrays handled currently!'
        cast = infer_parser(element_type)
        val = []
        for x in range(dim_info[0][1], dim_info[0][0]+1):
            el_size = struct.unpack('!i', value[offs:offs+4])[0]
            offs += 4
            if el_size == -1:
                val.append(None)
                continue
            val.append(cast.parse(value[offs:offs+el_size], size))
            offs += el_size
        return val


class NameArrayType(ArrayType):
    oid = 1003


class TextArray(ArrayType):
    oid = 1009


class BlankPaddedString(StringType):
    '''
    char(length), blank-padded string, fixed storage length
    '''
    oid = 1042


class VarCharType(StringType):
    oid = 1043


class DateType(BaseType):
    oid = 1082
    klass = datetime.date
    fmt = '!i'

    @classmethod
    def parse(cls, value, size):
        if not size:
            return None
        val = struct.unpack(cls.fmt, value[:size])[0]
        return datetime.date(2000, 1, 1) + datetime.timedelta(days=val)

    @classmethod
    def format(cls, value):
        val = (value - datetime.date(2000, 1, 1)).days
        return (1082, struct.pack(cls.fmt, val), cls.size)


class TimeOfDayType(BaseType):
    '''
    64bit int of uSec since midnight.
    '''
    oid = 1083
    klass = datetime.time
    fmt = '!q'

    @classmethod
    def parse(cls, value, size):
        time_us = struct.unpack(cls.fmt, value[:size])[0]
        val, microsecond = divmod(time_us, 1000000)
        val, second = divmod(val, 60)
        hour, minute = divmod(val, 60)
        return datetime.time(hour, minute, second, microsecond)

    @classmethod
    def format(cls, value):
        val = ((value.hour * 60 + value.minute) * 60 + value.second) * 1000000 + value.microsecond
        return (cls.oid, struct.pack(cls.fmt, val), cls.size)


class IntervalType(BaseType):
    oid = 1186
    klass = datetime.timedelta
    fmt = '!qii'

    @classmethod
    def parse(cls, value, size):
        if not size:
            return None
        time_us, days, months = struct.unpack(cls.fmt, value[:size])
        val = datetime.timedelta(days=days + months * 30, microseconds=time_us)
        return val

    @classmethod
    def format(cls, value):
        months, days = divmod(value.days, 30)
        usec = value.seconds * 1000000 + value.microseconds
        return (cls.oid, struct.pack(cls.fmt, usec, days, months), cls.size)


class TimestampTzType(BaseType):
    oid = 1184
    klass = datetime.datetime
    fmt = '!q'

    @classmethod
    def parse(cls, value, size):
        if size == 0:
            return None
        val = struct.unpack(cls.fmt, value[:size])[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(microseconds=val)
        return datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=val)

    @classmethod
    def format(cls, value):
        if value.tzinfo:
            val = (value - datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))
        else:
            val = (value - datetime.datetime(2000, 1, 1))
        val = int(val.total_seconds() * 1000000)
        return (1184, struct.pack(cls.fmt, val), cls.size)


class NumericType(BaseType):
    oid = 1700
    klass = Decimal

    @staticmethod
    def parse(value, size):
        if not size:
            return None
        hsize = struct.calcsize('!HhHH')
        ndigits, weight, sign, dscale = struct.unpack('!HhHH', value[:hsize])
        if sign == 0xc000:
            return Decimal('NaN')
        desc = '!%dH' % ndigits
        digits = struct.unpack(desc, value[hsize:hsize+struct.calcsize(desc)])
        n = '-' if sign else ''
        # numeric has a form of compression where if the remaining digits are 0
        # they are not sent, even if we haven't reached the decimal point yet!
        while weight >= len(digits):
            digits = digits + (0,)

        for idx, digit in enumerate(digits):
            n += '{:04d}'.format(digit)
            if idx == weight:
                n += '.'
        n = Decimal(n)
        return n

    @classmethod
    def format(cls, value):
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
        size = struct.calcsize(fmt)
        val = struct.pack(fmt , len(vals), max(0, weight-1), 0xc000 if sign else 0, dscale, *vals)
        return (cls.oid, val, size)


class UUIDType(BaseType):
    oid = 2950
    klass = uuid.UUID

    @staticmethod
    def parse(value, size):
        return uuid.UUID(bytes=value[:size])

    @classmethod
    def format(cls, value):
        return (cls.oid, value.bytes, 16)


class JsonbType(BaseType):
    oid = 3802

    @staticmethod
    def parse(value, size):
        if value[0] == b'\x01':
            return json.loads(value[1:size].decode('utf-8'))
        return value[1:size].decode('utf-8')
