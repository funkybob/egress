from ctypes import c_char_p

import datetime
import json
import struct
import uuid

from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address, IPv4Network, IPv6Network
from itertools import repeat


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
    def parse(cls, value, size, tzinfo):
        if size == 0:
            return None
        return struct.unpack(cls.fmt, value[:size])[0]

    @classmethod
    def format(cls, value):
        return (cls.oid, struct.pack(cls.fmt, value), cls.size)


class ArrayType(BaseType):

    @staticmethod
    def parse(value, size, tzinfo):
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
            val.append(cast.parse(value[offs:offs+el_size], el_size, tzinfo))
            offs += el_size
        return val


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
    def parse(value, size, tzinfo):
        if not size:
            return None
        return value[:size]

    @staticmethod
    def format(value):
        return (17, c_char_p(value), len(value))


class CharType(BaseType):
    oid = 18
    fmt = 'c'

    @staticmethod
    def parse(value, size, tzinfo):
        return value[:size].decode('utf-8')


class NameDataType(BaseType):
    oid = 19

    @staticmethod
    def parse(value, size, tzinfo):
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


class Int2VectorType(ArrayType):
    oid = 22


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
    blank_value = ''

    @classmethod
    def parse(cls, value, size, tzinfo):
        if not size:
            return cls.blank_value
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
    def parse(value, size, tzinfo):
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
    def parse(value, size, tzinfo):
        if not size:
            return None
        return value[:size].decode('utf-8')


class IPAddressType(IPv4AddressType):
    oid = 869


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
    def parse(cls, value, size, tzinfo):
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
    def parse(cls, value, size, tzinfo):
        if not size:
            return None
        time_us = struct.unpack(cls.fmt, value[:size])[0]
        val, microsecond = divmod(time_us, 1000000)
        val, second = divmod(val, 60)
        hour, minute = divmod(val, 60)
        return datetime.time(hour, minute, second, microsecond, tzinfo=tzinfo)

    @classmethod
    def format(cls, value):
        val = ((value.hour * 60 + value.minute) * 60 + value.second) * 1000000 + value.microsecond
        return (cls.oid, struct.pack(cls.fmt, val), cls.size)


class TimestampType(BaseType):
    oid = 1114
    fmt = '!q'

    @classmethod
    def parse(cls, value, size, tzinfo):
        if size == 0:
            return None
        val = struct.unpack(cls.fmt, value[:size])[0]
        return datetime.datetime(2000, 1, 1, tzinfo=tzinfo) + datetime.timedelta(microseconds=val)


class IntervalType(BaseType):
    oid = 1186
    klass = datetime.timedelta
    fmt = '!qii'

    @classmethod
    def parse(cls, value, size, tzinfo):
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
    def parse(cls, value, size, tzinfo):
        if size == 0:
            return None
        val = struct.unpack(cls.fmt, value[:size])[0]
        return datetime.datetime(2000, 1, 1, tzinfo=tzinfo) + datetime.timedelta(microseconds=val)

    @classmethod
    def format(cls, value):
        if value.tzinfo:
            val = (value - datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))
        else:
            val = (value - datetime.datetime(2000, 1, 1))
        val = int(val.total_seconds() * 1000000)
        return (1184, struct.pack(cls.fmt, val), cls.size)


class TimeTzType(BaseType):
    oid = 1266
    # klass = datetime.time
    fmt = '!q'

    @classmethod
    def parse(cls, value, size, tzinfo):
        time_us = struct.unpack(cls.fmt, value[:size])[0]
        val, microsecond = divmod(time_us, 1000000)
        val, second = divmod(val, 60)
        hour, minute = divmod(val, 60)
        return datetime.time(hour, minute, second, microsecond, tzinfo=tzinfo)

    @classmethod
    def format(cls, value):
        val = ((value.hour * 60 + value.minute) * 60 + value.second) * 1000000 + value.microsecond
        return (cls.oid, struct.pack(cls.fmt, val), cls.size)


class NumericType(BaseType):
    oid = 1700
    klass = Decimal

    @staticmethod
    def parse(value, size, tzinfo):
        if not size:
            return None
        hsize = struct.calcsize('!HhHH')
        ndigits, weight, sign, dscale = struct.unpack('!HhHH', value[:hsize])
        if sign == 0xc000:
            return Decimal('NaN')
        desc = '!%dH' % ndigits
        digits = struct.unpack(desc, value[hsize:hsize+struct.calcsize(desc)])

        def source(digits):
            '''
            A generator to yield the digits from the Digits, and then an
            infinite stream of '0's.
            '''
            for d in digits:
                dd = '{:04d}'.format(d)
                yield from dd
            yield from repeat('0')

        src = source(digits)

        n = '-' if sign else ''
        # numeric has a form of compression where if the remaining digits are 0
        # they are not sent, even if we haven't reached the decimal point yet!
        for _ in range((weight+1) * 4):
            n = n + next(src)
        if dscale:
            n += '.'
            for _ in range(dscale):
                n = n + next(src)
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
        val = struct.pack(fmt, len(vals), max(0, weight-1), 0x4000 if sign else 0, dscale, *vals)
        return (cls.oid, val, size)


class UUIDType(BaseType):
    oid = 2950
    klass = uuid.UUID

    @staticmethod
    def parse(value, size, tzinfo):
        return uuid.UUID(bytes=value[:size])

    @classmethod
    def format(cls, value):
        return (cls.oid, value.bytes, 16)


class JsonbType(BaseType):
    oid = 3802

    @staticmethod
    def parse(value, size, tzinfo):
        if value[0] == b'\x01':
            return json.loads(value[1:size].decode('utf-8'))
        return value[1:size].decode('utf-8')


# This type object is used to describe columns in a database that are
# string-based (e.g. CHAR).
STRING = StringType()

# This type object is used to describe (long) binary columns in a database
# (e.g. LONG, RAW, BLOBs).
BINARY = BinaryType()

# This type object is used to describe numeric columns in a database.
NUMBER = NumericType()

# This type object is used to describe date/time columns in a database.
DATETIME = TimestampTzType()

# This type object is used to describe the "Row ID" column in a database.
ROWID = int()
