db_name = 'test_egress_001'
create_db_command = 'create database %s'
drop_db_command = 'drop database %s'


def create_db(cursor, name=None):
    name = name or db_name
    cursor.execute(create_db_command % name)


def drop_db(cursor, name=None):
    name = name or db_name
    cursor.execute(drop_db_command % name)
