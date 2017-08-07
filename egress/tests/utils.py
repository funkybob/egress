prefix = 'test_egress_'
create_db_command = 'create database %sdb1'
drop_db_command = 'drop database %sdb1'


def create_db(cursor, dbname_prefix=None):
    dbname_prefix = dbname_prefix or prefix
    cursor.execute(create_db_command % dbname_prefix)


def drop_db(cursor, dbname_prefix=None):
    dbname_prefix = dbname_prefix or prefix
    cursor.execute(drop_db_command % dbname_prefix)
