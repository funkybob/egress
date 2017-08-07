'''
Tests for postgres numeric types
'''

import unittest

import egress as db
from egress.tests.test_config import DATABASE
from egress.tests.test_utils import create_db, drop_db


def connect():
    return db.connect(**DATABASE)


def setUpModule():
    '''Create a connection and a database'''
    connection = connect()
    create_db(connection.conn)


def tearDownModule():
    '''Drop the database and close the connection'''
    connection = connect()
    drop_db(connection.conn)
    connection.close()


class TestNumericTypes(unittest.TestCase):
    '''
    Tests for postgres numeric types
    '''
    def test_numeric(self):
        self.skipTest('TBD')
