from django.db.backends.postgresql.introspection import DatabaseIntrospection as _DatabaseIntrospection

from egress import types

class DatabaseIntrospection(_DatabaseIntrospection):
    data_types_reverse = {
        types.BoolType: 'BooleanField',
        types.BinaryType: 'BinaryField',
        types.LongType: 'BigIntegerField',
        types.ShortIntType: 'SmallIntegerField',
        types.IntType: 'IntegerField',
        types.StringType: 'TextField',
        types.FloatType: 'FloatField',
        types.DoubleType: 'FloatField',
        types.IPAddressType: 'GenericIPAddressField',
        types.IPv4AddressType: 'GenericIPAddressField',
        types.IPv4NetworkType: 'GenericIPAddressField',
        types.IPv6AddressType: 'GenericIPAddressField',
        types.IPv6NetworkType: 'GenericIPAddressField',
        types.BlankPaddedString: 'CharField',
        types.VarCharType: 'CharField',
        types.DateType: 'DateField',
        types.TimeOfDayType: 'TimeField',
        types.TimestampType: 'DateTimeField',
        types.TimestampTzType: 'DateTimeField',
        types.TimeTzType: 'TimeField',
        types.NumericType: 'DecimalField',
        types.UUIDType: 'UUIDField',
        types.JsonbType: 'JSONField',
        types.IntervalType: 'DurationField',
    }