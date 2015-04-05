# coding=utf8

"""
Python client for https://github.com/ideawu/ssdb
"""

__version__ = '0.1.7.1'
__license__ = 'bsd2'

import sys
import socket
import threading
import spp


if sys.version > '3':
    # binary: cast str to bytes
    binary = lambda string: bytes(string, 'utf8')
    # string: cast bytes to native string
    string = lambda binary: binary.decode('utf8')
else:
    binary = str
    string = str


class SSDBException(Exception):
    pass


class Connection(threading.local):

    def __init__(self, host='0.0.0.0', port=8888):
        super(Connection, self).__init__()
        self.host = host
        self.port = port
        self.commands = []
        self.sock = self.parser = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setblocking(1)
        self.sock.connect((self.host, self.port))
        self.parser = spp.Parser()

    def close(self):
        self.parser.clear()
        self.sock.close()
        self.sock = self.parser = None

    def request(self):
        # lazy connect
        if self.sock is None:
            self.connect()

        # send commands
        buf = ''.join(['%d\n%s\n' % (len(str(i)), str(i)) for i in self.commands] + ['\n'])
        self.sock.sendall(buf)

        chunk = None

        while 1:
            buf = self.sock.recv(4096)

            if not isinstance(buf, bytes) and not len(buf):
                self.close()
                raise socket.error('Socket closed on remote end')

            self.parser.feed(string(buf))
            chunk = self.parser.get()
            if chunk is not None:
                break

        cmd = self.commands[0]
        self.commands = []
        status, body = chunk[0], chunk[1:]

        if status == 'ok':
            return body[0] if len(body) == 1 else body
        elif status == 'not_found':
            return None
        else:
            raise SSDBException('%r on command %r', status, cmd)


class Client(object):

    def __init__(self, host='0.0.0.0', port=8888):
        super(Client, self).__init__()
        self.host = host
        self.port = port
        self.conn = Connection(host=host, port=port)

    def close(self):
        self.conn.close()

    def __getattr__(self, cmd):
        def create_method(command):
            def method(*args):
                self.conn.commands = (command, ) + args
                return self.conn.request()
            return method

        cmd = {'delete': 'del'}.get(cmd, cmd)
        if cmd not in self.__dict__:
            self.__dict__[cmd] = create_method(cmd)

        return self.__dict__[cmd]
