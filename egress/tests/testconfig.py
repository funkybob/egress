
# Configure the test suite from the env variables.

import os

dbuser = os.environ.get('EGRESS_TESTDB_USER', None)
dbhost = os.environ.get('EGRESS_TESTDB_HOST', None)
dbpassword = os.environ.get('EGRESS_TESTDB_PASSWORD', None)
dbport = os.environ.get('EGRESS_TESTDB_PORT', None)

DATABASES = {}
if dbuser:
    DATABASES['user'] = dbuser
if dbhost:
    DATABASES['host'] = dbhost
if dbpassword:
    DATABASES['password'] = dbpassword
if dbport:
    DATABASES['port'] = dbport
