# -*- coding: utf-8 -*-
import time
import json
import logging
import numpy as np
from gensim.models.doc2vec import Doc2Vec
from django.core.management.base import BaseCommand
from utils import (
    Tokenizer, PsqlQuery, tfidf_jaccard_similarity
)
from django.utils import timezone
from chat_app.models import JiebaTagWeight
from evaluate_app.metrics import doc2vec_ndcg


W2V_PATH = '/var/local/okbot/w2v/segtag-vec.bin'
D2V_PATH = '/var/local/okbot/doc2vec/tok2push.model'

logger = logging.getLogger('okbot_eval')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setLevel(logging.INFO)
ch.setFormatter(chformatter)
logger.addHandler(ch)

'''
每x秒query一次，算一次map
週期預設為每分鐘一次
1. map 實作
2. query 流程
    (1). 隨機選擇一篇文章
    (2). 保留該篇文章做為答案
    (3). 以該篇文章標題query剩餘文章
    (4). 取出最後推文集合，由答案計算map
    (5). tag, q-ref(tiles, urls, pushes), q-res(titles, urls), pushes, tokenizer, time, map-score

'''


class Command(BaseCommand):
    help = '''
           Evaluate retrieval engine.
           ex: ./manage.py okbot_eval --tokenizer=jieba --w2v=0 --jtag=0 --sample=1
           '''

    def add_arguments(self, parser):
        parser.add_argument('--sample', nargs=1, type=int)
        parser.add_argument('--tokenizer', nargs=1, type=str)
        parser.add_argument('--w2v', nargs=1, type=int)
        parser.add_argument('--jtag', nargs=1, type=int)
        pass

    def _parse_arg(self, name, default, options):
        try:
            return options[name][0]
        except:
            return default

    def handle(self, *args, **options):
        tok_tag = self._parse_arg('tokenizer', 'jieba', options)
        sample = self._parse_arg('sample', 1, options)
        w2v = self._parse_arg('w2v', 0, options) > 0
        jtag = self._parse_arg('jtag', 0, options) > 0
        print(tok_tag, sample, w2v, jtag)

        print('Loading doc2vec model...')
        d2v_model = Doc2Vec.load(D2V_PATH)
        print('Model loaded.')

        w2v_model = None
        if w2v:
            print('Loading word2vec model...')
            w2v_model = gensim.models.KeyedVectors.load_word2vec_format(W2V_PATH, binary=True, unicode_errors='ignore')
            print('Model loaded.')
        jiebatag_weight = {}
        if jtag:
            jtagweight = JiebaTagWeight.objects.all()
            for jt in jtagweight:
                jiebatag_weight[jt.name] = {'weight': jt.weight, 'punish': jt.punish_factor}
        

        evaluator = Evaluator()
        

        for _ in range(sample):
            evaluator.draw()
            raw_push = evaluator.get_predict_push(
                tokenizer=tok_tag, w2v_model=w2v_model, jiebatag_weight=jiebatag_weight
            )

            topic = evaluator.get_topic_field('tokenized')
            topic_words = Tokenizer(tok_tag).cut(topic, pos=False)
            predict_words_ls = [Tokenizer(tok_tag).cut(push, pos=False) for push in raw_push if 'http' not in push]
            print(topic_words)
            print(len(predict_words_ls))
            
            score = doc2vec_ndcg(topic_words, predict_words_ls, d2v_model)
            print(score)

        # jlpath, tok_tag = options['jlpath'][0], options['tokenizer'][0]

        # tokenizer = Tokenizer(tok_tag)

        # time_tic = time.time()
        # logger.info('okbot ingest job start. source: {}'.format(file_name))
        # now = timezone.now()
        # jobid =  '.'.join(file_name.split('.')[:3] + [now.strftime('%Y-%m-%d-%H-%M-%S')])
        # Joblog(name=jobid, start_time=now, status='running').save()

        # jlparser = CrawledJLParser(jlpath, tokenizer)
        # ingester = Ingester(spider_tag, tok_tag)

        # consumed_num = 0
        # for batch_post in jlparser.batch_parse():
            # if len(batch_post) > 0:
                # post_url = ingester.upsert_post(batch_post)
                # vocab_name = ingester.upsert_vocab_ignore_docfreq(batch_post)
                # ingester.upsert_vocab2post(batch_post, vocab_name, post_url)
                # consumed_num += len(batch_post)
            # logger.info('okbot ingest: {} data are consumed from {}.'.format(consumed_num, file_name))
        # elapsed_time = time.time() - time_tic
        # logger.info('okbot ingest job finished. elapsed time: {:.2f} sec.'.format(elapsed_time))
        # now = timezone.now()
        # try:
            # job = Joblog.objects.get(name=jobid)
            # job.finish_time = now
            # job.result = 'Total {} records are ingestered. elapsed time: {:.2f} sec.'.format(consumed_num, elapsed_time)
        # except Exception as e:
            # logger.error(e)
            # logger.error('command okbot_ingest, fail to fetch job log. id: {}. create a new one'.format(jobid))
            # job = Joblog(name=jobid, start_time=now)
            # job.result = e

        # finally:
        #     job.status = 'finished'
        #     job.save()


class Evaluator(object):
    logger = logging.getLogger('okbot_eval')

    jieba_tag_weight = {}
    w2v_model = None
    vocab_docfreq_th = 10000
    default_tokenizer = 'jieba'
    ranking_factor = 0.8
    max_query_post_num = 50000
    max_top_post_num = 10

    random_query_sql = '''
        SELECT id, title, tokenized, grammar, push, url FROM ingest_app_post TABLESAMPLE SYSTEM(0.01);
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

    def __init__(self):
        psql = PsqlQuery()
        self.topic_post = list(psql.query(self.random_query_sql))[0]
        self.topic_pschema = psql.schema


    def draw(self):
        psql = PsqlQuery()
        self.topic_post = list(psql.query(self.random_query_sql))[0]
        self.topic_pschema = psql.schema

    def _query_vocab(self, tokenizer='jieba', w2v_model=None, jiebatag_weight={}):
        words = self.topic_post[self.topic_pschema['tokenized']].split()
        flags = self.topic_post[self.topic_pschema['grammar']].split()

        # self.tok, self.words, self.flags = Tokenizer(tokenizer).cut(self.post[self.pschema['title']])

        vocab_name = ['--+--'.join([w, f, tokenizer]) for w, f in zip(words, flags)]
        vocab_score = {name: 1.0 for name in vocab_name}

        # Merge word2vec model here
        # ===============================
        if bool(w2v_model):
            try:
                w2v_query = ['{}:{}'.format(w, f) for w, f in zip(words, flags) if f[0] in ['v', 'n'] or f in ['eng']]
                if bool(w2v_query):
                    w2v_neighbor = w2v_model.most_similar(positive=w2v_query, topn=min(3, len(w2v_query)))

                    w2v_name = ['--+--'.join('{}:{}'.format(w[0], tokenizer).split(':')) for w in w2v_neighbor]
                    w2v_score = [w[1] for w in w2v_neighbor]

                    for name, score in zip(w2v_name, w2v_score):
                        vocab_score[name] = score

                    vocab_name.extend(w2v_name)
            except:
                self.logger.warning('word2vec query failed.')
                pass

        psql = PsqlQuery()
        qvocab = list(psql.query(self.query_vocab_sql, (tuple(vocab_name),)))

        vschema = psql.schema
        _tag_weight = {
            q[vschema['tag']]: jiebatag_weight[q[vschema['tag']]]['weight']
            if q[vschema['tag']] in jiebatag_weight else 1.0 for q in qvocab
        }
        # ===============================

        vocab = [
            {
                'word': ':'.join([q[vschema['word']], q[vschema['tag']]]),
                'termweight': _tag_weight[q[vschema['tag']]] * vocab_score[q[vschema['name']]],
                'docfreq': q[vschema['doc_freq']]
            } for q in qvocab
        ]

        # keyword = json.dumps(vocab, indent=4, ensure_ascii=False, sort_keys=True)
        # self.logger.info(keyword)

        vid = [
            q[vschema['id']]
            for q in qvocab
            if not (q[vschema['stopword']]) and q[vschema['doc_freq']] < self.vocab_docfreq_th
        ]

        return vocab, vid

    def _query_post(self, vid):

        _query_pid = list(PsqlQuery().query(
            self.query_vocab2post_sql, (tuple(vid),))
        )
        query_pid = [p[0] for p in _query_pid if p[0] != self.topic_post[self.topic_pschema['id']]]
        psql = PsqlQuery()
        allpost = psql.query(self.query_post_sql, (tuple(query_pid),))
        return allpost, psql.schema

    def _cal_similarity(self, vocab, allpost, pschema, scorer=tfidf_jaccard_similarity):
        post_buffer = []
        score_buffer = []

        for i, post in enumerate(allpost):
            if i >= self.max_query_post_num:
                break
            doc = [':'.join([t, g]) for t, g in zip(post[pschema['tokenized']].split(), post[pschema['grammar']].split())]
            post_buffer.append(post)
            score_buffer.append(scorer(vocab, doc))

        return post_buffer, score_buffer

    def _ranking_post(self, similar_post, similar_score, pschema):
        # TODO: add other feature weighting here
        # ======================================
        w_pushcount = 0.2
        w_pdate = 0.4
        w_similar = 5.0

        now = similar_post[0][pschema['publish_date']].timestamp()
        score = []
        for i, post in enumerate(similar_post):
            s = w_pushcount * len(post[pschema['push']].split('\n')) \
                + w_pdate * post[pschema['publish_date']].timestamp() / now \
                + w_similar * similar_score[i]

            score.append(s)

        idx_ranking = np.asarray(score).argsort()[::-1]
        top_post = []
        top_score = []
        max_score = score[idx_ranking[0]]
        for m, idx in enumerate(idx_ranking):
            if (score[idx] / max_score) < self.ranking_factor or m > self.max_top_post_num:
                break
            else:
                top_post.append(similar_post[idx])
                top_score.append(score[idx])
        # ======================================

        ref = []
        for p, s in zip(top_post, top_score):
            ref.append('[{:.2f}]{}\n{}'.format(s, p[pschema['tokenized']], p[pschema['url']]))

        post_ref = '\n\n'.join(ref)
        # self.logger.info('\n' + post_ref)

        return top_post, top_score, post_ref

    def _clean_push(self, top_post, post_score, pschema, verbose=True):
        push_pool = []

        for post, score in zip(top_post, post_score):
            union_push = {}
            anony_num = 0
            for line, mix in enumerate(post[pschema['push']].split('\n')):
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

        return push_pool

    def _ranking_push(self, push_pool, verbose=True):
        # TODO: ranking push
        # ==================
        idx_weight, len_weight = 2.0, 1.0

        push = []
        for pool in push_pool:
            push.extend(pool['push'])

        score = []
        for i, p in enumerate(push):
            score.append(idx_weight / (1 + i) - len_weight * len(p))

        idx_ranking = np.asarray(score).argsort()[::-1]

        top_push = [push[idx] for idx in idx_ranking]
        # for p in top_push:
        #    self.logger.info(p)

        push_num = len(top_push)
        # centre = push_num >> 1
        # pick = centre + centre * np.random.normal(0, 1) / 2.0
        self.logger.info('Push count: {}.'.format(push_num))
        # final_push = top_push[int(min(push_num - 1, max(0, pick)))]

        # ==================

        return top_push

    def get_relevant_push(self):
        post = self.topic_post
        pschema = self.topic_pschema

        push_pool = self._clean_push([post], [1.0], pschema, verbose=False)
        top_push = self._ranking_push(push_pool, verbose=False)
        return top_push

    def get_predict_push(self, tokenizer='jieba', w2v_model=None, jiebatag_weight={}):
        vocab, vid = self._query_vocab(
            tokenizer=tokenizer, w2v_model=w2v_model, jiebatag_weight=jiebatag_weight
        )
        allpost, pschema = self._query_post(vid)

        # slow
        similar_post, similar_score = self._cal_similarity(vocab, allpost, pschema)
        top_post, top_score, post_ref = self._ranking_post(similar_post, similar_score, pschema)
        
        push_pool = self._clean_push(top_post, top_score, pschema)
        top_push = self._ranking_push(push_pool)

        return top_push

    def get_topic_field(self, field='tokenized'):
        return self.topic_post[self.topic_pschema[field]]
