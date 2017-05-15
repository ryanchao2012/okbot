import os
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

from .models import ChatRule, JiebaTagWeight

from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    FollowEvent, ImageSendMessage, JoinEvent, LeaveEvent,
    MessageEvent, SourceGroup, SourceRoom, SourceUser,
    TextMessage, TextSendMessage, UnfollowEvent,
)


class Chat(object):
    logger = logging.getLogger('okbot_chat_view')
    tag_weight = {}

    w2v_model = None
    disclaimer = None
    activate_key = None
    activate_response = []

    repeat_time = 10
    repeat_cold_interval = 60
    repeat_response = []

    kickout_key = []
    kickout_response = []

    vocab_docfreq_th = 10000
    default_tokenizer = 'jieba'
    ranking_factor = 0.8
    max_query_post_num = 50000
    max_top_post_num = 5

    insert_chattree_sql = '''
        INSERT INTO chat_app_chattree(user_id, ancestor, query, keyword, reply, time, post, push_num)
        SELECT %(user_id)s, %(ancestor)s, %(query)s, %(keyword)s,
               %(reply)s, %(time)s, %(post)s, %(push_num)s
        RETURNING id;
    '''

    update_chattree_sql = '''
        UPDATE chat_app_chattree SET successor = %(successor)s WHERE id = %(id_)s;
    '''

    query_chatuser_sql = '''
        SELECT * FROM chat_app_chatuser
        WHERE uid = %(uid)s AND platform = %(platform)s;
    '''

    upsert_chatuser_sql = '''
        INSERT INTO chat_app_chatuser(platform, uid, idtype, active, state, chat_count)
        SELECT %(platform)s, %(uid)s, %(idtype)s, %(active)s, %(state)s, %(chat_count)s
        ON CONFLICT (platform, uid) DO
        UPDATE SET
        active = EXCLUDED.active,
        state = EXCLUDED.state,
        chat_count = chat_app_chatuser.chat_count + 1;
    '''

    query_chatcache_sql = '''
        SELECT * FROM chat_app_chatcache WHERE user_id = %s;
    '''

    upsert_chatcache_sql = '''
        INSERT INTO chat_app_chatcache(user_id, query, keyword, reply, time, repeat, post, push_num, tree_node)
        SELECT %(user_id)s, %(query)s, %(keyword)s, %(reply)s, %(time)s, %(repeat)s, %(post)s, %(push_num)s, %(tree_node)s
        ON CONFLICT (user_id) DO
        UPDATE SET
        query = EXCLUDED.query,
        keyword = EXCLUDED.keyword,
        reply = EXCLUDED.reply,
        time = EXCLUDED.time,
        repeat = EXCLUDED.repeat,
        post = EXCLUDED.post,
        push_num = EXCLUDED.push_num,
        tree_node = EXCLUDED.tree_node;
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

    def __init__(self, query, platform, uid, idtype='', tokenizer='jieba'):
        self.platform = platform
        self.uid = uid
        self.idtype = idtype
        self.event_time = timezone.now()
        self.query = query
        self.user, self.uschema = self._get_user()
        if not(bool(self.user)):
            self._upsert_user(active=False)
            self.user, self.uschema = self._get_user()

        self.cache, self.cschema = self._query_cache()
        self.post_ref = ''
        self.chat_tree_id = -1

        if self.user[self.uschema['active']]:
            self.tok, self.words, self.flags = Tokenizer(tokenizer).cut(query)

        if not bool(Chat.tag_weight):
            jtag = JiebaTagWeight.objects.all()
            for jt in jtag:
                Chat.tag_weight[jt.name] = {'weight': jt.weight, 'punish': jt.punish_factor}

        if not bool(Chat.w2v_model):
            self.logger.info('loading word2vec model...')
            Chat.w2v_model = gensim.models.KeyedVectors.load_word2vec_format('w2v/segtag-vec.bin', binary=True, unicode_errors='ignore')
            self.logger.info('loading completed')

        if not bool(Chat.disclaimer):
            disclaimer = ChatRule.objects.get(rtype='disclaimer')
            Chat.disclaimer = disclaimer.response

        if not bool(Chat.repeat_response):
            repeat = ChatRule.objects.get(rtype='repeat')
            Chat.repeat_response = repeat.response.split('\n')
            Chat.repeat_time = int(repeat.keyword)

        if not(bool(Chat.kickout_key) and bool(Chat.kickout_response)):
            kickout = ChatRule.objects.get(rtype='kickout')
            Chat.kickout_key = [k.strip() for k in kickout.keyword.split(',')]
            Chat.kickout_response = [r.strip() for r in kickout.response.split('\n')]

        if not (bool(Chat.activate_key) and bool(Chat.activate_response)):
            activate = ChatRule.objects.get(rtype='activate')
            Chat.activate_key = [k.strip() for k in activate.keyword.split(',')]
            Chat.activate_response = [r.strip() for r in activate.response.split('\n')]

    def _get_user(self):
        user, schema = None, {}
        psql = PsqlQuery()
        user_ = list(psql.query(self.query_chatuser_sql, {'uid': self.uid, 'platform': self.platform}))
        if bool(user_):
            user = user_[0]
            schema = psql.schema

        return user, schema

    def _insert_chattree(self, push):
        data = {
            'user_id': self.user[self.uschema['id']],
            'ancestor': self.cache[self.cschema['tree_node']],
            'query': self.query,
            'keyword': self.keyword,
            'reply': push,
            'time': self.event_time,
            'post': self.post_ref,
            'push_num': len(self.push_pool)
        }
        try:
            psql = PsqlQuery()
            self.chat_tree_id = psql.insert_with_col_return(self.insert_chattree_sql, data)
        except Exception as e:
                self.logger.error('Insert ChatTree failed: {}'.format(e))
    def _update_chattree(self):
        if bool(self.cache) and self.cache[self.cschema['tree_node']] > 0:
            try:
                psql = PsqlQuery()
                psql.upsert(self.update_chattree_sql, {'successor': self.chat_tree_id, 'id_': self.cache[self.cschema['tree_node']]})
            except Exception as e:
                self.logger.error('Update ChatTree failed: {}'.format(e))

    def _upsert_user(self, active=False, state=0):
        psql = PsqlQuery()
        data = {
            'platform': self.platform,
            'uid': self.uid,
            'idtype': self.idtype,
            'active': active,
            'state': state,
            'chat_count': 0
        }
        try:
            psql.upsert(self.upsert_chatuser_sql, data)
        except Exception as e:
            self.logger.error('Upsert ChatUser failed: {}'.format(e))

    def _query_cache(self):
        cache, schema = None, {}
        psql = PsqlQuery()
        try:
            cache_ = list(psql.query(self.query_chatcache_sql, (self.user[self.uschema['id']],)))
            if bool(cache_):
                cache = cache_[0]
                schema = psql.schema

        except Exception as e:
            self.logger.warning(e)

        return cache, schema

    def _upsert_cache(self, push):
        if bool(self.user):
            repeat = 0
            if bool(self.cache):
                if self.cache[self.cschema['query']].strip() == self.query.strip():
                    repeat = self.cache[self.cschema['repeat']] + 1

            psql = PsqlQuery()
            data = {
                'user_id': self.user[self.uschema['id']],
                'query': self.query,
                'keyword': self.keyword,
                'reply': push,
                'time': self.event_time,
                'repeat': repeat,
                'post': self.post_ref,
                'push_num': len(self.push_pool),
                'tree_node': self.chat_tree_id
            }

            try:
                psql.upsert(self.upsert_chatcache_sql, data)
            except Exception as e:
                self.logger.error('Upsert ChatCache failed: {}'.format(e))

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

        ref = []
        for p, s in zip(top_post, top_score):
            ref.append('[{:.2f}]{}\n{}'.format(s, p[self.pschema['tokenized']], p[self.pschema['url']]))
        self.post_ref = '\n\n'.join(ref)
        self.logger.info(self.post_ref)

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

    def _chat(self):
        try:
            self._query_vocab(w2v=True)
            self._query_post()
            self._cal_similarity()
            self._ranking_post()
            self._clean_push()
            push = self._ranking_push()

            self._insert_chattree(push)
            self._update_chattree()
            self._upsert_cache(push)
            

        except Exception as e:
            default_reply = ['嗄', '三小', '滾喇', '嘻嘻']
            push = default_reply[random.randint(0, len(default_reply) - 1)]
            self.logger.error(e)
            self.logger.warning('Query failed: {}'.format(self.query))
        finally:
            pass # self._upsert_cache(push)

        return push

    def retrieve(self):
        if self.user[self.uschema['active']]:
            if bool(self.cache) \
                and self.cache[self.cschema['repeat']] > Chat.repeat_time \
                and self.cache[self.cschema['query']].strip() == self.query.strip() \
                and (self.event_time.timestamp() - self.cache[self.cschema['time']].timestamp()) < Chat.repeat_cold_interval:

                l = len(Chat.repeat_response)
                reply = Chat.repeat_response[random.randint(0, l - 1)]

            else:
                reply = self._chat()

            self._upsert_user(active=True)

        else:
            if self.query in Chat.activate_key:
                self._upsert_user(active=True)
                l = len(Chat.activate_response)
                reply = Chat.activate_response[random.randint(0, l - 1)]
            else:
                reply = Chat.disclaimer

        return reply.strip()


class MessengerBot(Chat):
    pass


class LineBot(Chat):
    code_leave = 1
    code_normal = 0
    line_bot_api = LineBotApi(os.environ['LINE_CHANNEL_ACCESS_TOKEN'])
    line_webhook_parser = WebhookParser(os.environ['LINE_CHANNEL_SECRET'])

    def retrieve(self):
        if self.query in Chat.kickout_key and self.idtype != 'user':
            l = len(Chat.kickout_response)
            return Chat.kickout_response[random.randint(0, l - 1)], LineBot.code_leave
        else:
            return super(LineBot, self).retrieve(), LineBot.code_normal


    def leave(self):
        try:
            if self.idtype == 'group':
                LineBot.line_bot_api.leave_group(self.uid)
            elif self.idtype == 'room':
                LineBot.line_bot_api.leave_room(self.uid)

        except LineBotApiError as err:
            logger.error('okbot.chat_app.bots.LineBot.leave, message: {}'.format(err))

        self._upsert_user(active=False)
