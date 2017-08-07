
# Configure the test suite from the env variables.

import os

dbuser = os.environ.get('EGRESS_TESTDB_USER', None)
dbhost = os.environ.get('EGRESS_TESTDB_HOST', None)
dbpassword = os.environ.get('EGRESS_TESTDB_PASSWORD', None)
dbport = os.environ.get('EGRESS_TESTDB_PORT', None)

DATABASE = {}
if dbuser:
    DATABASE['user'] = dbuser
if dbhost:
    DATABASE['host'] = dbhost
if dbpassword:
    DATABASE['password'] = dbpassword
if dbport:
    DATABASE['port'] = dbport
