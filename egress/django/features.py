from django.db.backends.postgresql.features import DatabaseFeatures as _DatabaseFeatures


class DatabaseFeatures(_DatabaseFeatures):
    supports_paramstyle_pyformat = False
    supports_timezones = False
