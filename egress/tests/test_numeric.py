'''
Tests for postgres numeric types
'''

import unittest

import egress as db
from egress.tests.config import DATABASE
from egress.tests.utils import create_db, drop_db


def connect():
    return db.connect(**DATABASE)


def setUpModule():
    '''Create a connection and a database'''
    connection = connect()
    connection._autocommit = True
    with connection.cursor() as cursor:
        create_db(cursor)


def tearDownModule():
    '''Drop the database and close the connection'''
    connection = connect()
    connection._autocommit = True
    with connection.cursor() as cursor:
        drop_db(cursor)

    connection.close()


class TestNumericTypes(unittest.TestCase):
    '''
    Tests for postgres numeric types
    '''
    def test_numeric(self):
        self.skipTest('TBD')
