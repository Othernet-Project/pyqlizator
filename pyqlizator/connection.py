import socket

import msgpack

from .cursor import Cursor
from .exceptions import Error


class Socket(object):

    def __init__(self, host, port, timeout=2):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._sock.settimeout(timeout)

    def close(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    def recv(self, buf_size=4096):
        while True:
            buf = self._sock.recv(buf_size)
            if not buf:
                return
            # yield received chunks immediately instead of collecting
            # all together so that streaming parsers can be utilized
            yield buf
            # in case the received package was smaller than `buf_size`
            # prevent the generator from doing any more work
            if len(buf) < buf_size:
                return

    def send(self, data):
        self._sock.sendall(data)


class Connection(object):
    # client error codes
    NETWORK_ERROR = 100
    SERVER_ERROR = 101

    socket_cls = Socket
    cursor_cls = Cursor

    def __init__(self, host, port, database, path, **options):
        self._dbname = database
        self._dbpath = path
        try:
            self._socket = self.socket_cls(host, port)
        except (socket.error, socket.timeout) as exc:
            self._socket = None
            raise Error(self.NETWORK_ERROR, str(exc), original_exception=exc)
        else:
            self._connect_to_database(**options)

    def _send(self, data):
        serialized = msgpack.packb(data, default=self.cursor_cls.to_primitive)
        try:
            self._socket.send(serialized)
        except (socket.error, socket.timeout) as exc:
            self._socket = None
            raise Error(self.NETWORK_ERROR, str(exc), original_exception=exc)

    def _recv(self):
        unpacker = msgpack.Unpacker()
        try:
            for data in self._socket.recv():
                unpacker.feed(data)
                for obj in unpacker:
                    yield obj
        except (socket.error, socket.timeout) as exc:
            self._socket = None
            raise Error(self.NETWORK_ERROR, str(exc), original_exception=exc)

    def transmit(self, data):
        self._send(data)
        return self._recv()

    @property
    def closed(self):
        return self._socket is None

    def close(self):
        self._socket.close()
        self._socket = None

    def cursor(self):
        return self.cursor_cls(self)

    def _check_status(self, reply):
        try:
            (header,) = reply
        except ValueError:
            raise Error(self.SERVER_ERROR, 'unrecognized reply')
        else:
            retval = header.get('status')
            if retval != 0:
                message = header.get('message', 'no error message')
                details = header.get('details', 'no details')
                raise Error(retval, message, details=details)

    def _connect_to_database(self, **options):
        data = {'endpoint': 'connect', 'database': self._dbpath}
        data.update(options)
        reply = self.transmit(data)
        self._check_status(reply)

    def drop_database(self):
        data = {'endpoint': 'drop', 'database': self._dbpath}
        reply = self.transmit(data)
        self._check_status(reply)

    @property
    def database(self):
        return self._dbpath
