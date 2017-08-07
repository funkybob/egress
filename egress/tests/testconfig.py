
# Configure the test suite from the env variables.

import os

DATABASES = {
    'user': os.environ.get('EGRESS_TESTDB_USER', 'postgres'),
    'host': os.environ.get('EGRESS_TESTDB_HOST', 'localhost'),
    'password': os.environ.get('EGRESS_TESTDB_PASSWORD', None),
    'port': '',
}
