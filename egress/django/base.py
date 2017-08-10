import sys

from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
# from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
# from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from django.db.backends.postgresql.client import DatabaseClient
# from django.db.backends.postgresql.creation import DatabaseCreation
from django.db.backends.postgresql.features import DatabaseFeatures as _DatabaseFeatures
from django.db.backends.postgresql.introspection import DatabaseIntrospection
# from django.db.backends.postgresql.operations import DatabaseOperations
# from django.db.backends.postgresql.schema import DatabaseSchemaEditor

from django.conf import settings

import egress as Database


class DatabaseFeatures(_DatabaseFeatures):
    supports_paramstyle_pyformat = False
    supports_timezones = False


class DatabaseCreation(BaseDatabaseCreation):

    def _quote_name(self, name):
        return self.connection.ops.quote_name(name)

    def _get_database_create_suffix(self, encoding=None, template=None):
        suffix = ""
        if encoding:
            suffix += " ENCODING '{}'".format(encoding)
        if template:
            suffix += " TEMPLATE {}".format(self._quote_name(template))
        if suffix:
            suffix = "WITH" + suffix
        return suffix

    def sql_table_creation_suffix(self):
        test_settings = self.connection.settings_dict['TEST']
        assert test_settings['COLLATION'] is None, (
            "PostgreSQL does not support collation setting at database creation time."
        )
        return self._get_database_create_suffix(
            encoding=test_settings['CHARSET'],
            template=test_settings.get('TEMPLATE'),
        )

    def _clone_test_db(self, number, verbosity, keepdb=False):
        # CREATE DATABASE ... WITH TEMPLATE ... requires closing connections
        # to the template database.
        self.connection.close()

        source_database_name = self.connection.settings_dict['NAME']
        target_database_name = self.get_test_db_clone_settings(number)['NAME']
        test_db_params = {
            'dbname': self._quote_name(target_database_name),
            'suffix': self._get_database_create_suffix(template=source_database_name),
        }
        with self._nodb_connection.cursor() as cursor:
            try:
                self._execute_create_test_db(cursor, test_db_params, keepdb)
            except Exception as e:
                try:
                    if verbosity >= 1:
                        print("Destroying old test database for alias %s..." % (
                            self._get_database_display_str(verbosity, target_database_name),
                        ))
                    cursor.execute('DROP DATABASE %(dbname)s' % test_db_params)
                    self._execute_create_test_db(cursor, test_db_params, keepdb)
                except Exception as e:
                    sys.stderr.write("Got an error cloning the test database: %s\n" % e)
                    sys.exit(2)


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

    sql_alter_column_type = "ALTER COLUMN %(column)s TYPE %(type)s USING %(column)s::%(type)s"

    sql_create_sequence = "CREATE SEQUENCE %(sequence)s"
    sql_delete_sequence = "DROP SEQUENCE IF EXISTS %(sequence)s CASCADE"
    sql_set_sequence_max = "SELECT setval('%(sequence)s', MAX(%(column)s)) FROM %(table)s"

    sql_create_index = "CREATE INDEX %(name)s ON %(table)s%(using)s (%(columns)s)%(extra)s"
    sql_create_varchar_index = "CREATE INDEX %(name)s ON %(table)s (%(columns)s varchar_pattern_ops)%(extra)s"
    sql_create_text_index = "CREATE INDEX %(name)s ON %(table)s (%(columns)s text_pattern_ops)%(extra)s"
    sql_delete_index = "DROP INDEX IF EXISTS %(name)s"

    # Setting the constraint to IMMEDIATE runs any deferred checks to allow
    # dropping it in the same transaction.
    sql_delete_fk = "SET CONSTRAINTS %(name)s IMMEDIATE; ALTER TABLE %(table)s DROP CONSTRAINT %(name)s"

    # def quote_value(self, value):
    #     return psycopg2.extensions.adapt(value)

    def _field_indexes_sql(self, model, field):
        output = super()._field_indexes_sql(model, field)
        like_index_statement = self._create_like_index_sql(model, field)
        if like_index_statement is not None:
            output.append(like_index_statement)
        return output

    def _create_like_index_sql(self, model, field):
        """
        Return the statement to create an index with varchar operator pattern
        when the column type is 'varchar' or 'text', otherwise return None.
        """
        db_type = field.db_type(connection=self.connection)
        if db_type is not None and (field.db_index or field.unique):
            # Fields with database column types of `varchar` and `text` need
            # a second index that specifies their operator class, which is
            # needed when performing correct LIKE queries outside the
            # C locale. See #12234.
            #
            # The same doesn't apply to array fields such as varchar[size]
            # and text[size], so skip them.
            if '[' in db_type:
                return None
            if db_type.startswith('varchar'):
                return self._create_index_sql(model, [field], suffix='_like', sql=self.sql_create_varchar_index)
            elif db_type.startswith('text'):
                return self._create_index_sql(model, [field], suffix='_like', sql=self.sql_create_text_index)
        return None

    def _alter_column_type_sql(self, model, old_field, new_field, new_type):
        """Make ALTER TYPE with SERIAL make sense."""
        table = model._meta.db_table
        if new_type.lower() in ("serial", "bigserial"):
            column = new_field.column
            sequence_name = "%s_%s_seq" % (table, column)
            col_type = "integer" if new_type.lower() == "serial" else "bigint"
            return (
                (
                    self.sql_alter_column_type % {
                        "column": self.quote_name(column),
                        "type": col_type,
                    },
                    [],
                ),
                [
                    (
                        self.sql_delete_sequence % {
                            "sequence": self.quote_name(sequence_name),
                        },
                        [],
                    ),
                    (
                        self.sql_create_sequence % {
                            "sequence": self.quote_name(sequence_name),
                        },
                        [],
                    ),
                    (
                        self.sql_alter_column % {
                            "table": self.quote_name(table),
                            "changes": self.sql_alter_column_default % {
                                "column": self.quote_name(column),
                                "default": "nextval('%s')" % self.quote_name(sequence_name),
                            }
                        },
                        [],
                    ),
                    (
                        self.sql_set_sequence_max % {
                            "table": self.quote_name(table),
                            "column": self.quote_name(column),
                            "sequence": self.quote_name(sequence_name),
                        },
                        [],
                    ),
                ],
            )
        else:
            return super()._alter_column_type_sql(model, old_field, new_field, new_type)

    def _alter_field(self, model, old_field, new_field, old_type, new_type,
                     old_db_params, new_db_params, strict=False):
        # Drop indexes on varchar/text columns that are changing to a different
        # type.
        if (old_field.db_index or old_field.unique) and (
            (old_type.startswith('varchar') and not new_type.startswith('varchar')) or
            (old_type.startswith('text') and not new_type.startswith('text'))
        ):
            index_name = self._create_index_name(model._meta.db_table, [old_field.column], suffix='_like')
            self.execute(self._delete_constraint_sql(self.sql_delete_index, model, index_name))

        super()._alter_field(
            model, old_field, new_field, old_type, new_type, old_db_params,
            new_db_params, strict,
        )
        # Added an index? Create any PostgreSQL-specific indexes.
        if ((not (old_field.db_index or old_field.unique) and new_field.db_index) or
                (not old_field.unique and new_field.unique)):
            like_index_statement = self._create_like_index_sql(model, new_field)
            if like_index_statement is not None:
                self.execute(like_index_statement)

        # Removed an index? Drop any PostgreSQL-specific indexes.
        if old_field.unique and not (new_field.db_index or new_field.unique):
            index_to_remove = self._create_index_name(model._meta.db_table, [old_field.column], suffix='_like')
            self.execute(self._delete_constraint_sql(self.sql_delete_index, model, index_to_remove))


class DatabaseOperations(BaseDatabaseOperations):
    cast_char_field_without_max_length = 'varchar'

    def unification_cast_sql(self, output_field):
        internal_type = output_field.get_internal_type()
        if internal_type in ("GenericIPAddressField", "IPAddressField", "TimeField", "UUIDField"):
            # PostgreSQL will resolve a union as type 'text' if input types are
            # 'unknown'.
            # https://www.postgresql.org/docs/current/static/typeconv-union-case.html
            # These fields cannot be implicitly cast back in the default
            # PostgreSQL configuration so we need to explicitly cast them.
            # We must also remove components of the type within brackets:
            # varchar(255) -> varchar.
            return 'CAST(%%s AS %s)' % output_field.db_type(self.connection).split('(')[0]
        return '%s'

    def date_extract_sql(self, lookup_type, field_name):
        # https://www.postgresql.org/docs/current/static/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
        if lookup_type == 'week_day':
            # For consistency across backends, we return Sunday=1, Saturday=7.
            return "EXTRACT('dow' FROM %s) + 1" % field_name
        else:
            return "EXTRACT('%s' FROM %s)" % (lookup_type, field_name)

    def date_trunc_sql(self, lookup_type, field_name):
        # https://www.postgresql.org/docs/current/static/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
        return "DATE_TRUNC('%s', %s)" % (lookup_type, field_name)

    def _convert_field_to_tz(self, field_name, tzname):
        if settings.USE_TZ:
            field_name = "%s AT TIME ZONE '%s'" % (field_name, tzname)
        return field_name

    def datetime_cast_date_sql(self, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        return '(%s)::date' % field_name

    def datetime_cast_time_sql(self, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        return '(%s)::time' % field_name

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        return self.date_extract_sql(lookup_type, field_name)

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        # https://www.postgresql.org/docs/current/static/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
        return "DATE_TRUNC('%s', %s)" % (lookup_type, field_name)

    def time_trunc_sql(self, lookup_type, field_name):
        return "DATE_TRUNC('%s', %s)::time" % (lookup_type, field_name)

    def deferrable_sql(self):
        return " DEFERRABLE INITIALLY DEFERRED"

    def fetch_returned_insert_ids(self, cursor):
        """
        Given a cursor object that has just performed an INSERT...RETURNING
        statement into a table that has an auto-incrementing ID, return the
        list of newly created IDs.
        """
        return [item[0] for item in cursor.fetchall()]

    def lookup_cast(self, lookup_type, internal_type=None):
        lookup = '%s'

        # Cast text lookups to text to allow things like filter(x__contains=4)
        if lookup_type in ('iexact', 'contains', 'icontains', 'startswith',
                           'istartswith', 'endswith', 'iendswith', 'regex', 'iregex'):
            if internal_type in ('IPAddressField', 'GenericIPAddressField'):
                lookup = "HOST(%s)"
            else:
                lookup = "%s::text"

        # Use UPPER(x) for case-insensitive lookups; it's faster.
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            lookup = 'UPPER(%s)' % lookup

        return lookup

    def no_limit_value(self):
        return None

    def prepare_sql_script(self, sql):
        return [sql]

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name  # Quoting once is enough.
        return '"%s"' % name

    def set_time_zone_sql(self):
        return "SET TIME ZONE %s"

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        if tables:
            # Perform a single SQL 'TRUNCATE x, y, z...;' statement.  It allows
            # us to truncate tables referenced by a foreign key in any other
            # table.
            tables_sql = ', '.join(
                style.SQL_FIELD(self.quote_name(table)) for table in tables)
            if allow_cascade:
                sql = ['%s %s %s;' % (
                    style.SQL_KEYWORD('TRUNCATE'),
                    tables_sql,
                    style.SQL_KEYWORD('CASCADE'),
                )]
            else:
                sql = ['%s %s;' % (
                    style.SQL_KEYWORD('TRUNCATE'),
                    tables_sql,
                )]
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
            return sql
        else:
            return []

    def sequence_reset_by_name_sql(self, style, sequences):
        # 'ALTER SEQUENCE sequence_name RESTART WITH 1;'... style SQL statements
        # to reset sequence indices
        sql = []
        for sequence_info in sequences:
            table_name = sequence_info['table']
            column_name = sequence_info['column']
            if not (column_name and len(column_name) > 0):
                # This will be the case if it's an m2m using an autogenerated
                # intermediate table (see BaseDatabaseIntrospection.sequence_list)
                column_name = 'id'
            sql.append("%s setval(pg_get_serial_sequence('%s','%s'), 1, false);" % (
                style.SQL_KEYWORD('SELECT'),
                style.SQL_TABLE(self.quote_name(table_name)),
                style.SQL_FIELD(column_name),
            ))
        return sql

    def tablespace_sql(self, tablespace, inline=False):
        if inline:
            return "USING INDEX TABLESPACE %s" % self.quote_name(tablespace)
        else:
            return "TABLESPACE %s" % self.quote_name(tablespace)

    def sequence_reset_sql(self, style, model_list):
        from django.db import models
        output = []
        qn = self.quote_name
        for model in model_list:
            # Use `coalesce` to set the sequence for each model to the max pk value if there are records,
            # or 1 if there are none. Set the `is_called` property (the third argument to `setval`) to true
            # if there are records (as the max pk value is already in use), otherwise set it to false.
            # Use pg_get_serial_sequence to get the underlying sequence name from the table name
            # and column name (available since PostgreSQL 8)

            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    output.append(
                        "%s setval(pg_get_serial_sequence('%s','%s'), "
                        "coalesce(max(%s), 1), max(%s) %s null) %s %s;" % (
                            style.SQL_KEYWORD('SELECT'),
                            style.SQL_TABLE(qn(model._meta.db_table)),
                            style.SQL_FIELD(f.column),
                            style.SQL_FIELD(qn(f.column)),
                            style.SQL_FIELD(qn(f.column)),
                            style.SQL_KEYWORD('IS NOT'),
                            style.SQL_KEYWORD('FROM'),
                            style.SQL_TABLE(qn(model._meta.db_table)),
                        )
                    )
                    break  # Only one AutoField is allowed per model, so don't bother continuing.
            for f in model._meta.many_to_many:
                if not f.remote_field.through:
                    output.append(
                        "%s setval(pg_get_serial_sequence('%s','%s'), "
                        "coalesce(max(%s), 1), max(%s) %s null) %s %s;" % (
                            style.SQL_KEYWORD('SELECT'),
                            style.SQL_TABLE(qn(f.m2m_db_table())),
                            style.SQL_FIELD('id'),
                            style.SQL_FIELD(qn('id')),
                            style.SQL_FIELD(qn('id')),
                            style.SQL_KEYWORD('IS NOT'),
                            style.SQL_KEYWORD('FROM'),
                            style.SQL_TABLE(qn(f.m2m_db_table()))
                        )
                    )
        return output

    def prep_for_iexact_query(self, x):
        return x

    def max_name_length(self):
        """
        Return the maximum length of an identifier.

        The maximum length of an identifier is 63 by default, but can be
        changed by recompiling PostgreSQL after editing the NAMEDATALEN
        macro in src/include/pg_config_manual.h.

        This implementation returns 63, but can be overridden by a custom
        database backend that inherits most of its behavior from this one.
        """
        return 63

    def distinct_sql(self, fields):
        if fields:
            return 'DISTINCT ON (%s)' % ', '.join(fields)
        else:
            return 'DISTINCT'

    def last_executed_query(self, cursor, sql, params):
        # http://initd.org/psycopg/docs/cursor.html#cursor.query
        # The query attribute is a Psycopg extension to the DB API 2.0.
        if cursor.query is not None:
            return cursor.query.decode()
        return None

    def return_insert_id(self):
        return "RETURNING %s", ()

    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def adapt_datefield_value(self, value):
        return value

    def adapt_datetimefield_value(self, value):
        return value

    def adapt_timefield_value(self, value):
        return value

    # def adapt_ipaddressfield_value(self, value):
    #     if value:
    #         return Inet(value)
    #     return None

    def subtract_temporals(self, internal_type, lhs, rhs):
        if internal_type == 'DateField':
            lhs_sql, lhs_params = lhs
            rhs_sql, rhs_params = rhs
            return "(interval '1 day' * (%s - %s))" % (lhs_sql, rhs_sql), lhs_params + rhs_params
        return super().subtract_temporals(internal_type, lhs, rhs)


class DatabaseWrapper(BaseDatabaseWrapper):

    vendor = 'postgres'
    display_name = 'PostgreSQL (egress)'

    Database = Database

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    SchemaEditorClass = DatabaseSchemaEditor

    data_types = {
        'AutoField': 'serial',
        'BigAutoField': 'bigserial',
        'BinaryField': 'bytea',
        'BooleanField': 'boolean',
        'CharField': 'varchar(%(max_length)s)',
        'DateField': 'date',
        'DateTimeField': 'timestamp with time zone',
        'DecimalField': 'numeric(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'interval',
        'FileField': 'varchar(%(max_length)s)',
        'FilePathField': 'varchar(%(max_length)s)',
        'FloatField': 'double precision',
        'IntegerField': 'integer',
        'BigIntegerField': 'bigint',
        'IPAddressField': 'inet',
        'GenericIPAddressField': 'inet',
        'NullBooleanField': 'boolean',
        'OneToOneField': 'integer',
        'PositiveIntegerField': 'integer',
        'PositiveSmallIntegerField': 'smallint',
        'SlugField': 'varchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField': 'text',
        'TimeField': 'time',
        'UUIDField': 'uuid',
    }
    data_type_check_constraints = {
        'PositiveIntegerField': '"%(column)s" >= 0',
        'PositiveSmallIntegerField': '"%(column)s" >= 0',
    }
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }

    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%'",
        'icontains': "LIKE '%%' || UPPER({}) || '%%'",
        'startswith': "LIKE {} || '%%'",
        'istartswith': "LIKE UPPER({}) || '%%'",
        'endswith': "LIKE '%%' || {}",
        'iendswith': "LIKE '%%' || UPPER({})",
    }

    def get_new_connection(self, conn_params):
        connection = Database.connect(**conn_params)
        with connection.cursor() as cur:
            cur.execute('SET TIME ZONE UTC;')
            connection.commit()
        return connection

    def _set_autocommit(self, autocommit):
        self.connection._autocommit = autocommit

    def init_connection_state(self):
        pass

    def get_connection_params(self):
        settings_dict = self.settings_dict
        # None may be used to connect to the default 'postgres' db
        if settings_dict['NAME'] == '':
            raise ImproperlyConfigured("settings.DATABASES is improperly configured. Please supply the NAME value.")
        conn_params = {
            'dbname': settings_dict['NAME'] or 'postgres',
        }
        conn_params.update(settings_dict['OPTIONS'])
        conn_params.pop('isolation_level', None)
        if settings_dict['USER']:
            conn_params['user'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']
        if settings_dict['HOST']:
            conn_params['host'] = settings_dict['HOST']
        if settings_dict['PORT']:
            conn_params['port'] = settings_dict['PORT']
        return conn_params

    def create_cursor(self, name=None):
        return self.connection.cursor()

    def is_usable(self):
        try:
            # Use a psycopg cursor directly, bypassing Django's utilities.
            self.connection.cursor().execute("SELECT 1")
        except Database.Error:
            return False
        else:
            return True
