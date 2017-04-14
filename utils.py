import psycopg2
import time
import os


class PsqlQuery(object):
    OKBOT_DB_USER = os.environ['OKBOT_DB_USER']
    OKBOT_DB_NAME = os.environ['OKBOT_DB_NAME']
    OKBOT_DB_PASSWORD = os.environ['OKBOT_DB_PASSWORD']

    def __init__(self, username=None, db=None, password=None):
        self.user = username or self.OKBOT_DB_USER
        self.db = db or self.OKBOT_DB_NAME
        self.pw = password or self.OKBOT_DB_PASSWORD
        self.schema = {}

    def execute(self, query_string, query_tuple=None):
        self._get_schema(query_string, query_tuple)
        return self._execute(query_string, query_tuple)

    def _get_schema(self, query_string, query_tuple=None):
        connect = psycopg2.connect(database=self.db, user=self.user, password=self.pw)
        cursor = connect.cursor()
        idx_semic = query_string.find(';')
        if idx_semic > 0:
            query_string = query_string[:idx_semic]
        danger_query = query_string + ' LIMIT 0'
        print(danger_query)
        print(cursor.mogrify(danger_query, query_tuple))
        cursor.execute(danger_query, query_tuple)
        schema = [desc[0] for desc in cursor.description]
        print('Warning: schema changed:', schema)
        cursor.close()
        connect.close()
        self.schema = {k: v for v, k in enumerate(schema)}
    def _execute(self, query_string, query_tuple=None):
        connect = psycopg2.connect(database=self.db, user=self.user, password=self.pw)
        cursor = connect.cursor()
        cursor.execute(query_string, query_tuple)
#        first = cursor.fetchone()
#        self.schema = 1 # [desc[0] for desc in cursor.description]
#        print(self.schema)
#        yield first
        for record in cursor:
            yield record
        cursor.close()
        connect.close()

