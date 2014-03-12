
'''
The module exports the following constructors and singletons:
'''

def Date(year, month, day):
    '''
    This function constructs an object holding a date value.
    '''
def Time(hour, minute, second):
    '''
    This function constructs an object holding a time value.
    '''

def Timestamp(year, month, day, hour, minute, second):
    '''
    This function constructs an object holding a time stamp value.
    '''

def DateFromTicks(ticks):
    '''
    This function constructs an object holding a date value from the given
    ticks value (number of seconds since the epoch; see the documentation of the
    standard Python time module for details).
    '''

def TimeFromTicks(ticks):
    '''
    This function constructs an object holding a time value from the given
    ticks value (number of seconds since the epoch; see the documentation of the
    standard Python time module for details).
    '''

def TimestampFromTicks(ticks):
    '''
    This function constructs an object holding a time stamp value from the
    given ticks value (number of seconds since the epoch; see the documentation
    of the standard Python time module for details).
    '''

def Binary(string):
    '''
    This function constructs an object capable of holding a binary (long) string
    value.
    '''

class DBAPITypeObject(object):
    '''Copied from PEP-249'''
    def __init__(self, *values):
        self.values = values
    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

# This type object is used to describe columns in a database that are
# string-based (e.g. CHAR).
STRING = DBAPITypeObject()

# This type object is used to describe (long) binary columns in a database (e.g.
# LONG, RAW, BLOBs).
BINARY = DBAPITypeObject()

# This type object is used to describe numeric columns in a database.
NUMBER = DBAPITypeObject()

# This type object is used to describe date/time columns in a database.
DATETIME = DBAPITypeObject()

# This type object is used to describe the "Row ID" column in a database.
ROWID = DBAPITypeObject()

