from django.db.backends.postgresql.features import DatabaseFeatures as _DatabaseFeatures


class DatabaseFeatures(_DatabaseFeatures):
    supports_paramstyle_pyformat = False
    # TEMPORARY!
    has_jsonb_datatype = False