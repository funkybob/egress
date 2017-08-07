'''
Tests for postgres numeric types
'''

import unittest
import egress as db
from .testconfig import DATABASES


table_prefix = 'test_egress_'
create_db_command = 'create database %sdb1' % table_prefix
drop_db_command = 'drop database %sdb1' % table_prefix


def connect():
    return db.connect(**DATABASES)


def setUpModule():
    '''Create a connection and a database'''
    connection = connect()
    create_db(connection.conn)


def tearDownModule():
    '''Drop the database and close the connection'''
    connection = connect()
    drop_db(connection.conn)
    connection.close()


def create_db(cursor):
    cursor.execute(create_db_command)


def drop_db(cursor):
    cursor.execute(drop_db_command)


class TestNumericTypes(unittest.TestCase):
    '''
    Tests for postgres numeric types
    '''
    def test_numeric(self):
        self.skipTest('TBD')
