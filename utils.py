import psycopg2
import time
import os


class PsqlAbstract(object):
    DB_USER = os.environ['OKBOT_DB_USER']
    DB_NAME = os.environ['OKBOT_DB_NAME']
    DB_PASSWORD = os.environ['OKBOT_DB_PASSWORD']

    def __init__(self, username=None, db=None, password=None):
        self.user = username or self.DB_USER
        self.db = db or self.DB_NAME
        self.pw = password or self.DB_PASSWORD

    def session(func):
        def _wrapper(self, *args, **kwargs):
            connect = psycopg2.connect(database=self.db, user=self.user, password=self.pw)
            cursor = connect.cursor()
            ret = func(self, cursor, **kwargs)
            cursor.close()
            connect.close()
            return ret

        return _wrapper

class PsqlQuery(PsqlAbstract):
    # OKBOT_DB_USER = os.environ['OKBOT_DB_USER']
    # OKBOT_DB_NAME = os.environ['OKBOT_DB_NAME']
    # OKBOT_DB_PASSWORD = os.environ['OKBOT_DB_PASSWORD']

    def __init__(self, username=None, db=None, password=None):
        super(self.__class__, self).__init__(username=username, db=db, password=password)
        self.schema = {}

    def query(self, q, data=None, skip=False):
        if not skip:
            self._get_schema(query_=q, data=data)
        return self._query(query_=q, data=data)

    @PsqlAbstract.session
    def _get_schema(self, cursor, query_=None, data=None):
        if query_ is None:
            return
        # connect = psycopg2.connect(database=self.db, user=self.user, password=self.pw)
        # cursor = connect.cursor()
        idx_semicln = query_.find(';')
        if idx_semicln > 0:
            query_ = query_[:idx_semicln]
        query_ += ' LIMIT 0;'
        cursor.execute(query_, data)
        schema = [desc[0] for desc in cursor.description]
        # print('Warning: schema changed:', schema)
        # cursor.close()
        # connect.close()
        self.schema = {k: v for v, k in enumerate(schema)}

    @PsqlAbstract.session
    def _query(self, cursor, query_=None, data=None):
        if query_ is None:
            return
        # connect = psycopg2.connect(database=self.db, user=self.user, password=self.pw)
        # cursor = connect.cursor()
        cursor.execute(query_, data)
        for record in cursor:
            yield record
        # cursor.close()
        # connect.close()

