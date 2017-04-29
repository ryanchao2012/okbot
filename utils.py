import psycopg2
import jieba.posseg as pseg
import math
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

    @staticmethod
    def session(keep=False):
        def _session(func):
            def _wrapper(self, *args, **kwargs):
                connect = psycopg2.connect(database=self.db, user=self.user, password=self.pw)
                cursor = connect.cursor()
                ret = func(self, connect, cursor, **kwargs)
                if not keep:
                    PsqlAbstract._close(connect, cursor)

                return ret

            return _wrapper
        return _session

    @staticmethod
    def _close(connect, cursor):
        cursor.close()
        connect.close()

    def _execute(self, sql_string, data):
        connect = psycopg2.connect(database=self.db, user=self.user, password=self.pw)
        cursor = connect.cursor()
        cursor.execute(sql_string, data)
        ret = cursor.fetchone()
        PsqlAbstract._close(connect, cursor)
        return ret


class PsqlQuery(PsqlAbstract):

    def __init__(self, username=None, db=None, password=None):
        super(self.__class__, self).__init__(username=username, db=db, password=password)
        self.schema = {}

    def upsert(self, q, data=None):
        self._upsert(query_=q, data=data)

    @PsqlAbstract.session()
    def _upsert(self, connect, cursor, query_=None, data=None):
        cursor.execute(query_, data)
        connect.commit()

    def delete(self, q, data=None):
        self._delete(query_=q, data=data)

    @PsqlAbstract.session()
    def _delete(self, connect, cursor, query_=None, data=None):
        cursor.execute(query_, data)
        connect.commit()


    def query(self, q, data=None, skip=False):
        if not skip:
            self._get_schema(query_=q, data=data)
        return self._query(query_=q, data=data)

    @PsqlAbstract.session()
    def _get_schema(self, connect, cursor, query_=None, data=None):
        if query_ is None:
            return
        idx_semicln = query_.find(';')
        if idx_semicln > 0:
            query_ = query_[:idx_semicln]
        query_ += ' LIMIT 0;'
        cursor.execute(query_, data)
        schema = [desc[0] for desc in cursor.description]
        # print('Warning: schema changed:', schema)
        self.schema = {k: v for v, k in enumerate(schema)}

    @PsqlAbstract.session(keep=True)
    def _query(self, connect, cursor, query_=None, data=None):
        if query_ is None:
            return
        cursor.execute(query_, data)
        for record in cursor:
            yield record
        PsqlAbstract._close(connect, cursor)



class TokenizerNotExistException(Exception):
    pass

class Tokenizer(object):

    _tokenizer = ('jieba',)

    def __init__(self, tok_tag):
        if tok_tag not in self._tokenizer:
            raise TokenizerNotExistException
        self.tok_tag = tok_tag

    def cut(self, sentence):
        if self.tok_tag == 'jieba':
            pairs = pseg.cut(sentence)
            tok, words, flags = [], [], []

            for p in pairs:
                w = p.word.strip()
                if len(w) > 0:
                    tok.append(p)
                    words.append(w)
                    flags.append(p.flag)
            return tok, words, flags



# summation(tf * (k1 + 1) /(tf + k1*(1 - b + b*len(doc)/AVE_DOC_LEN)))
# k1 = [1.2, 2.0]
def bm25_similarity(vocab, doc, k1=1.5, b=0.75):
    DOC_NUM = 300000.0
    AVE_TITLE_LEN = 19.0
    doc_len = len(doc)
    def _bm25(v):
        if v['word'] in doc:
            idf = math.log(DOC_NUM / min(1.0, v['docfreq']))
            tf = v['termweight']
            return idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b*doc_len/AVE_TITLE_LEN))
        else:
            return 0.0
    
    score = sum([_bm25(v) for v in vocab])

    return score
        

def tfidf_jaccard_similarity(vocab, doc):
    DOC_NUM = 300000
    invocab = []
    for v in vocab:
        if v['word'] in doc and v not in invocab:
            invocab.append(v)

    tfidf = [v['termweight'] * math.log(DOC_NUM / min(1.0, v['docfreq'])) for v in invocab]
    union = set([v['word'] for v in vocab] + doc)
    score = sum(tfidf) / float(len(union))
    return score


def jaccard_similarity(vocab, doc):
    wlist = [v['word'] for v in vocab]
    wset = set(wlist)
    dset = set(doc)
    union = set(wlist + doc)
    score = len(wset.intersection(dset)) / float(len(union))
    return score




class Chat(object):
    def __init__(self, query, tokenizer='jieba'):
        self.query = query
        self.tok, self.words, self.flags = Tokenizer(tokenizer).cut(query)
        vocab_name = ['--+--'.join([t.word, t.flag, 'jieba']) for t in tok]
        query_vocab = list(psql.query( '''
                                                SELECT * FROM ingest_app_vocabulary
                                                WHERE name IN %s;
                                            ''', (tuple(vocab_name),)
                                    )
        )

        self.vschema = psql.schema
        # self.vocab = [{'word': ':'.join([q[vschema['word']], q[vschema['tag']]]),'termweight': 1.0, 'docfreq': q[vschema['doc_freq']]} for q in query_vocab]
