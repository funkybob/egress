'''
Tests for postgres numeric types
'''

import unittest

import egress as db
from egress.tests.config import DATABASE
from egress.tests.utils import create_db, drop_db

db_name = 'test_egress_db'


def connect(dbname=None):
    '''Connect to a postgres server, if dbname then connect to the database'''
    if db_name:
        DATABASE['dbname'] = dbname

    return db.connect(**DATABASE)


def setUpModule():
    '''Create a connection and a database'''
    connection = connect()
    connection._autocommit = True
    with connection.cursor() as cursor:
        create_db(cursor, db_name)


def tearDownModule():
    '''Drop the database and close the connection'''
    connection = connect()
    connection._autocommit = True
    with connection.cursor() as cursor:
        drop_db(cursor, db_name)


class TestNumericTypes(unittest.TestCase):
    '''
    Tests for postgres numeric types
    '''
    def execute_operation_on_db(self, operation):
        connection = connect(dbname=db_name)
        connection._autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(operation)
            return cursor.rowcount

    def setUp(self):
        create_table = """
        CREATE TABLE bookings (best_price numeric(10,2))
        """
        self.execute_operation_on_db(create_table)

    def tearDown(self):
        create_table = """DROP TABLE bookings"""
        self.execute_operation_on_db(create_table)

    def test_insert_numeric_ok(self):
        insert_numeric = """
        INSERT INTO bookings (best_price) VALUES (1000.02)
        """
        result = self.execute_operation_on_db(insert_numeric)
        self.assertEqual(result, 1)
