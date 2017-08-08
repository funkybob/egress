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
    @staticmethod
    def execute_operation_on_db(operation):
        connection = connect(dbname=db_name)
        connection._autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(operation)
            return cursor.rowcount

    @classmethod
    def setUpClass(cls):
        create_table = """
        CREATE TABLE bookings (best_price numeric(10,2))
        """
        cls.execute_operation_on_db(create_table)

    @classmethod
    def tearDownClass(cls):
        create_table = """DROP TABLE bookings"""
        cls.execute_operation_on_db(create_table)

    def test_insert_numeric_ok(self):
        insert_numeric = """
        INSERT INTO bookings (best_price) VALUES (1000.02)
        """
        result = self.execute_operation_on_db(insert_numeric)
        self.assertEqual(result, 1)
