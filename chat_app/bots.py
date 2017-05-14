import json
import logging
import random

from django.utils import timezone

import gensim

import numpy as np

from utils import (
    PsqlQuery, Tokenizer,
    tfidf_jaccard_similarity
)

from .models import JiebaTagWeight


class Chat(object):
    logger = logging.getLogger('okbot_chat_view')
    tag_weight = {}
    w2v_model = {}
    vocab_docfreq_th = 10000
    default_tokenizer = 'jieba'
    ranking_factor = 0.9
    max_query_post_num = 50000
    max_top_post_num = 5

    query_chatcache_sql = '''
        SELECT * FROM chat_app_chatcache
        WHERE uid = %(uid)s AND platform = %(platform)s;
    '''

    upsert_chatcache_sql = '''
        INSERT INTO chat_app_chatcache
        SELECT %(platform)s, %(uid)s, %(idtype)s, %(query)s,
               %(keyword)s, %(reply)s, %(time)s
        ON CONFLICT (uid) DO
        UPDATE SET
            query = EXCLUDED.query,
            keyword = EXCLUDED.keyword,
            reply = EXCLUDED.reply,
            time = EXCLUDED.time
        WHERE platform = EXCLUDED.platform;
    '''

    query_vocab_sql = '''
        SELECT * FROM ingest_app_vocabulary WHERE name IN %s;
    '''
    query_vocab2post_sql = '''
        SELECT post_id FROM ingest_app_vocabulary_post
        WHERE vocabulary_id IN %s;
    '''
    query_post_sql = '''
        SELECT tokenized, grammar, push, url, publish_date
        FROM ingest_app_post WHERE id IN %s AND spider != 'mentalk'
        ORDER BY publish_date DESC;
    '''

    def _pre_rulecheck(self, raw):
        refined, action = raw, 0
        return refined, action

    def __init__(self, query, platform, uid, idtype='', tokenizer='jieba'):
        self.platform = platform
        self.uid = uid
        self.idtype = idtype
        self.event_time = timezone.now()
        # self.query, action = self._pre_rulecheck(query)
        self.tok, self.words, self.flags = Tokenizer(tokenizer).cut(query)

        if not bool(Chat.tag_weight):
            jtag = JiebaTagWeight.objects.all()
            for jt in jtag:
                Chat.tag_weight[jt.name] = {'weight': jt.weight, 'punish': jt.punish_factor}

        if not bool(Chat.w2v_model):
            self.logger.info('loading word2vec model...')
            Chat.w2v_model = gensim.models.KeyedVectors.load_word2vec_format('w2v/segtag-vec.bin', binary=True, unicode_errors='ignore')
            self.logger.info('loading completed')

    def _query_cache(self, uid, platform):
        psql = PsqlQuery()
        try:
            cache = list(psql.query(self.query_vocab_sql, {'uid': self.uid, 'platform': self.platform}))
            self.logger.info(cache)
        except Exception as e:
            self.logger.warning(e)

# SELECT %(platform)s, %(uid)s, %(idtype)s, %(query)s,
#                %(keyword)s, %(reply)s, %(time)s
    def _upsert_cache(self, push):
        psql = PsqlQuery()
        data = {
            'platform': self.platform,
            'uid': self.uid,
            'idtype': self.idtype,
            'query': self.query,
            'keyword': self.keyword,
            'reply': push,
            'time': self.event_time
        }
        try:
            psql.upsert(self.upsert_chatcache_sql, data)
        except Exception as e:
            self.logger.warning(e)

    def _query_vocab(self, w2v=False):
        vocab_name = ['--+--'.join([t.word, t.flag, self.default_tokenizer]) for t in self.tok]
        vocab_score = {name: 1.0 for name in vocab_name}

        # TODO: merge word2vec model here
        # ===============================
        if w2v and bool(Chat.w2v_model):
            try:
                w2v_query = ['{}:{}'.format(word, flag) for word, flag in zip(self.words, self.flags) if flag[0] in ['v', 'n'] or flag == 'eng']
                if bool(w2v_query):
                    w2v_neighbor = Chat.w2v_model.most_similar(positive=w2v_query, topn=min(3, len(w2v_query)))

                    w2v_name = ['--+--'.join('{}:{}'.format(w[0], self.default_tokenizer).split(':')) for w in w2v_neighbor]
                    w2v_score = [w[1] for w in w2v_neighbor]

                    for name, score in zip(w2v_name, w2v_score):
                        vocab_score[name] = score

                    vocab_name.extend(w2v_name)
            except:
                pass

        psql = PsqlQuery()
        qvocab = list(psql.query(self.query_vocab_sql, (tuple(vocab_name),)))

        vschema = psql.schema
        _tag_weight = {
            q[vschema['tag']]: Chat.tag_weight[q[vschema['tag']]]['weight']
            if q[vschema['tag']] in Chat.tag_weight else 1.0 for q in qvocab
        }
        # ===============================
        self.vocab = [
            {
                'word': ':'.join([q[vschema['word']], q[vschema['tag']]]),
                'termweight': _tag_weight[q[vschema['tag']]] * vocab_score[q[vschema['name']]],
                'docfreq': q[vschema['doc_freq']]
            } for q in qvocab
        ]

        self.vid = [
            q[vschema['id']]
            for q in qvocab
            if not (q[vschema['stopword']]) and q[vschema['doc_freq']] < self.vocab_docfreq_th
        ]

    def _query_post(self):
        self.keyword = json.dumps(self.vocab, indent=4, ensure_ascii=False, sort_keys=True)
        self.logger.info(self.keyword)

        query_pid = list(PsqlQuery().query(
            self.query_vocab2post_sql, (tuple(self.vid),))
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
        w_pushcount = 0.2
        w_pdate = 0.4
        w_similar = 5.0
        now = self.similar_post[0][self.pschema['publish_date']].timestamp()
        score = []
        for i, post in enumerate(self.similar_post):
            s = w_pushcount * len(post[self.pschema['push']].split('\n')) \
                + w_pdate * post[self.pschema['publish_date']].timestamp() / now \
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
            self.logger.info('[{:.2f}]{} {}'.format(s, p[self.pschema['tokenized']], p[self.pschema['url']]))

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
                    audience, push = mix[:idx].strip(), mix[idx + 1:].strip()

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
            score.append(idx_weight / (1 + i) - len_weight * len(p))

        idx_ranking = np.asarray(score).argsort()[::-1]

        top_push = [push[idx] for idx in idx_ranking]
        # for p in top_push:
        #    self.logger.info(p)

        push_num = len(top_push)
        centre = push_num >> 1
        pick = centre + centre * np.random.normal(0, 1) / 2.0
        self.logger.info('len: {}, centre: {}, pick: {}'.format(push_num, centre, pick))
        final_push = top_push[int(min(push_num - 1, max(0, pick)))]

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

            self._upsert_cache(push)
        except Exception as e:
            default_reply = ['嗄', '三小', '滾喇', '嘻嘻']
            push = default_reply[random.randint(0, len(default_reply) - 1)]
            self.logger.error(e)
            self.logger.warning('Query failed: {}'.format(self.query))

        return push


class MessengerBot(Chat):
    pass


class LineBot(Chat):
    pass
