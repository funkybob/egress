
import sys

from . import compat

sys.modules['psycopg2'] = compat
