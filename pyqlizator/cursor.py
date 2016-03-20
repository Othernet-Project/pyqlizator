from .exceptions import Error


class Cursor(object):
    # server commands
    EXECUTE = 1
    FETCH = 2
    # server reply status codes
    OK = 0
    UNKNOWN_ERROR = 1
    INVALID_REQUEST = 2
    DESERIALIZATION_ERROR = 3
    DATABASE_OPENING_ERROR = 4
    DATABASE_NOT_FOUND = 5
    INVALID_QUERY = 5

    _to_primitive_converters = {}
    _from_primitive_converters = {}

    def __init__(self, connection):
        self._conn = connection
        self._rowcount = -1
        self._cols = None

    @classmethod
    def to_primitive(cls, obj):
        try:
            fn = cls._to_primitive_converters[type(obj)]
        except KeyError:
            return obj
        else:
            return fn(obj)

    @classmethod
    def from_primitive(cls, value, type_name):
        try:
            fn = cls._from_primitive_converters[type_name]
        except KeyError:
            return value
        else:
            return fn(value)

    @classmethod
    def register_to_primitive(cls, type_object, fn):
        cls._to_primitive_converters[type_object] = fn

    @classmethod
    def register_from_primitive(cls, type_name, fn):
        cls._from_primitive_converters[type_name] = fn

    @property
    def connection(self):
        return self._conn

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def description(self):
        if not self._cols:
            return None
        return tuple((colname, coltype, None, None, None, None, None)
                     for (colname, coltype) in self._cols)

    def _process_header(self, header):
        # a dict containing ``status`` key holds the information about
        # whether the query was successful or not
        status = header.get('status', self.UNKNOWN_ERROR)
        if status != self.OK:
            message = header.get('message', 'no error message')
            raise Error(status, message)

        self._rowcount = header.get('rowcount', -1)
        self._cols = header['columns']

    def _process_data(self, data):
        return dict((colname, self.from_primitive(value, coltype))
                    for ((colname, coltype), value) in zip(self._cols, data))

    def _lazy_query(self, data):
        is_header_processed = False
        for obj in self._conn.transmit(data):
            # first object coming out is guaranteed to be the header
            if not is_header_processed:
                self._process_header(obj)
                is_header_processed = True
                continue
            yield self._process_data(obj)

    def _query(self, is_lazy, operation, sql, *parameters):
        try:
            (params,) = parameters
        except ValueError:
            params = ()

        data = {'endpoint': 'query',
                'operation': operation,
                'database': self._conn.database,
                'query': sql,
                'parameters': params}
        if not is_lazy:
            return list(self._lazy_query(data))
        return self._lazy_query(data)

    def execute(self, sql, *parameters):
        return self._query(False, self.EXECUTE, sql, *parameters)

    def executemany(self, sql, seq_of_params):
        return [self.execute(sql, params) for params in seq_of_params]

    def executescript(self, sql):
        return self.execute(sql)

    def fetchone(self, sql, *parameters):
        for item in self._query(True, self.FETCH, sql, *parameters):
            return item  # returns first item received and ignores rest

    def fetchall(self, sql, *parameters):
        return self._query(False, self.FETCH, sql, *parameters)

    def fetchiter(self, sql, *parameters):
        return self._query(True, self.FETCH, sql, *parameters)
