import psycopg2
from settings import POSTGRES, POSTGRES_SOURCES


class _DB:
    def __init__(self):
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(**POSTGRES)
        self.cur = self.conn.cursor()
        self.conn.autocommit = True


class _DBSource(_DB):
    def _connect(self):
        self.conn = psycopg2.connect(**POSTGRES_SOURCES)
        self.cur = self.conn.cursor()
        self.conn.autocommit = True


_db = _DB()
_db_source = _DBSource()
