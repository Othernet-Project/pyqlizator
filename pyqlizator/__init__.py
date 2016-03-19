from .connection import Connection
from .cursor import Cursor
from .exceptions import Error


MAX_VARIABLE_NUMBER = 999

to_primitive_converter = Cursor.register_to_primitive
from_primitive_converter = Cursor.register_from_primitive
