import psycopg2
import jieba.posseg as pseg
import math
import numpy as np
import time
import random
import os
import json
import logging
import gensim

from chat_app.models import JiebaTagWeight

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
    logger = logging.getLogger('okbot_chat_view')
    tag_weight = {}
    w2v_model = {}
    vocab_docfreq_th = 10000
    default_tokenizer = 'jieba'
    ranking_factor = 0.8
    max_query_post_num = 15000
    max_top_post_num = 5

    query_vocab_sql = '''
        SELECT * FROM ingest_app_vocabulary WHERE name IN %s;
    '''
    query_vocab2post_sql = '''
        SELECT post_id FROM ingest_app_vocabulary_post 
        WHERE vocabulary_id IN %s;
    '''
    query_post_sql = '''
        SELECT tokenized, grammar, push, url, publish_date
        FROM ingest_app_post WHERE id IN %s
        ORDER BY publish_date DESC;
    '''


    def _pre_rulecheck(self, raw):
        refined, action = raw, 0
        return refined, action


    def __init__(self, query, tokenizer='jieba'):
        self.query, action = self._pre_rulecheck(query)
        self.tok, self.words, self.flags = Tokenizer(tokenizer).cut(query)

        if not bool(Chat.tag_weight):
            jtag = JiebaTagWeight.objects.all()
            for jt in jtag: 
                Chat.tag_weight[jt.name] = {'weight': jt.weight, 'punish': jt.punish_factor}


        if not bool(Chat.w2v_model):
            self.logger.info('loading word2vec model...')
            Chat.w2v_model = gensim.models.KeyedVectors.load_word2vec_format('w2v/segtag-vec.bin', binary=True, unicode_errors='ignore')
            self.logger.info('loading completed')


    def _query_vocab(self, w2v=False):
        
        vocab_name = ['--+--'.join([t.word, t.flag, self.default_tokenizer]) for t in self.tok]
        vocab_score = {vocab_name: 1.0 for name in vocab_name}

        # TODO: merge word2vec model here
        # ===============================
        if w2v and bool(Chat.w2v_model):
            w2v_query = ['{}:{}'.format(word, flag) for word, flag in zip(self.words, self.flags) if flag[0] in ['v', 'n']]
            w2v_neighbor = Chat.w2v_model.most_similar(positive=w2v_query, topn=min(3, len(w2v_query)))

            w2v_name = ['--+--'.join('{}:{}'.format(w[0], self.default_tokenizer).split(':')) for w in w2v_neighbor]
            w2v_score = [w[1] for w in w2v_neighbor]

            for name, score in zip(w2v_name, w2v_score):
                vocab_score[name] = score

            vocab_name.extend(w2v_name)

        psql = PsqlQuery()
        qvocab = list(psql.query(self.query_vocab_sql, (tuple(vocab_name),)))
        vschema = psql.schema

        _tag_weight = {
            q[vschema['tag']]: Chat.tag_weight[q[vschema['tag']]]['weight'] * vocab_score[q[vschema['name']]]
            if q[vschema['tag']] in Chat.tag_weight else vocab_score[q[vschema['name']]] for q in qvocab
        }
        # ===============================

        self.vocab = [
            {
                'word': ':'.join([q[vschema['word']], q[vschema['tag']]]),
                'termweight': _tag_weight[q[vschema['tag']]], 
                'docfreq': q[vschema['doc_freq']]
            } for q in qvocab
        ]

        self.vid = [
            q[vschema['id']] 
            for q in qvocab 
            if not (q[vschema['stopword']]) and q[vschema['doc_freq']] < self.vocab_docfreq_th 
        ]


    def _query_post(self):
        vocab_json = json.dumps(self.vocab, indent=4, ensure_ascii=False, sort_keys=True)
        self.logger.info(vocab_json)

        query_pid = list(PsqlQuery().query(
                        self.query_vocab2post_sql, (tuple(self.vid),)
                    )
        )
        psql = PsqlQuery()
        self.allpost = psql.query(self.query_post_sql, (tuple(query_pid),))
        self.pschema = psql.schema


    def _cal_similarity(self, scorer=tfidf_jaccard_similarity):
        post_buffer = []
        score_buffer = []

        for i, post in enumerate(self.allpost):
            if i >= self.max_query_post_num:
                break
            doc = [':'.join([t, g]) for t, g in zip(post[self.pschema['tokenized']].split(), post[self.pschema['grammar']].split())]
            post_buffer.append(post)
            score_buffer.append(scorer(self.vocab, doc))

        self.similar_post = post_buffer
        self.similar_score = score_buffer       


    def _ranking_post(self):
        # TODO: add other feature weighting here
        # ======================================
        w_pushcount = 1.0, w_pdate = 2.0, w_similar = 1.5
        now = self.similar_post[0][self.pschema['publish_date']].timestamp()
        score = []
        for i, post in enumerate(self.similar_post):
            s = w_pushcount * len(post[self.pschema['push']].split('\n'))
              + w_pdate * post[self.pschema['publish_date']].timestamp() / now
              + w_similar * self.similar_score[i]

            score.append(s)

        idx_ranking = np.asarray(score).argsort()[::-1]
        top_post = []
        top_score = []
        max_score = score[idx_ranking[0]]
        for m, idx in enumerate(idx_ranking):
            if (score[idx] / max_score) < self.ranking_factor or m > self.max_top_post_num:
                break
            else:
                top_post.append(self.similar_post[idx])
                top_score.append(score[idx])
        # ======================================

        self.top_post = top_post
        self.post_score = top_score

        for p, s in zip(top_post, top_score):
            logger.info('[{:.2f}]{} {}'.format(s, p[self.pschema['tokenized']], p[self.pschema['url']]))


    def _clean_push(self):
        
        push_pool = []

        for post, score in zip(self.top_post, self.post_score):
            union_push = {}
            anony_num = 0
            for line, mix in enumerate(post[self.pschema['push']].split('\n')):
                idx = mix.find(':')
                if idx < 0:
                    anony_num += 1
                    name = 'anony@' + str(anony_num)
                    union_push[name] = {}
                    union_push[name]['push'] = [{'content': mix.strip(), 'line': line}]
                else:
                    audience, push = mix[:idx].strip(), mix[idx+1:].strip()

                    # TODO: add blacklist
                    # ====================


                    # ====================
                    if audience in union_push:
                        union_push[audience]['push'].append({'content': push, 'line': line})
                    else:
                        union_push[audience] = {}
                        union_push[audience]['push'] = [{'content': push, 'line': line}]

            for key, allpush in union_push.items():
                appendpush = []
                line = -10
                for p in allpush['push']:

                    if (p['line'] - line) < 2:
                        appendpush[-1] += p['content']

                    else:
                        appendpush.append(p['content'])

                    line = p['line']

                push_pool.append({'push': appendpush, 'post_score': score})

        self.push_pool = push_pool

        print('@@@len(push_pool)', len(push_pool))


    def _ranking_push(self):
        # TODO: ranking push
        # ==================
        idx_weight, len_weight = 2.0, 1.0

        push = []
        for pool in self.push_pool:
            push.extend(pool['push'])

        score = []
        for i, p in enumerate(push):
            score.append( idx_weight / (1 + i) - len_weight * len(p))

        idx_ranking = np.asarray(score).argsort()[::-1]

        top_push = [push[idx] for idx in idx_ranking]
        push_num = len(top_push) - 1
        final_push = top_push[int(min(push_num, push_num * abs(np.random.normal(0,1) / 4.0)))]

        # ==================

        return final_push

    def retrieve(self):
        try:
            self._query_vocab(w2v=True)
            self._query_post()
            self._cal_similarity()
            self._ranking_post()
            self._clean_push()
            push = self._ranking_push()
        except Exception as e:
            default_reply = ['嗄', '三小', '滾喇', '嘻嘻']
            push = default_reply[random.randint(0, len(default_reply)-1)]
            self.logger.error(e)
            self.logger.warning('Query failed: {}'.format(self.query))

        return push





class MessengerBot(Chat):
    pass


class LineBot(Chat):
    pass


