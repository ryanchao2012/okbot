# -*- coding: utf-8 -*-
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
import jieba.posseg as pseg
import collections
import json
import time
import os
import re

from ingest_app.models import Joblog
from utils import PsqlQuery, Tokenizer



import logging
logger = logging.getLogger('okbot_ingest')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setFormatter(chformatter)
logger.addHandler(ch)


class Command(BaseCommand):
    help = '''
           Ingest the crawled data into database.
           ex: python manage.py okbot_ingest --jlpath <jsonline-file> --tokenizer <tokenizer>
           '''

    def add_arguments(self, parser):
        parser.add_argument('--jlpath', nargs=1, type=str)
        parser.add_argument('--tokenizer', nargs=1, type=str)

    def handle(self, *args, **options):
        jlpath, tok_tag = options['jlpath'][0], options['tokenizer'][0]
        
        file_name = jlpath.split('/')[-1]
        spider_tag = file_name[: file_name.find('.')]
        tokenizer = Tokenizer(tok_tag)

        time_tic = time.time()
        logger.info('okbot ingest job start. source: {}'.format(file_name))
        now = timezone.now()
        jobid =  '.'.join(file_name.split('.')[:3] + [now.strftime('%Y-%m-%d-%H-%M-%S')])
        Joblog(name=jobid, start_time=now, status='running').save()

        jlparser = CrawledJLParser(jlpath, tokenizer)
        ingester = Ingester(spider_tag, tok_tag)

        consumed_num = 0
        for batch_post in jlparser.batch_parse():
            post_url = ingester.upsert_post(batch_post)
            vocab_name = ingester.upsert_vocab_ignore_docfreq(batch_post)
            ingester.upsert_vocab2post(batch_post, vocab_name, post_url)
            consumed_num += len(batch_post)
            logger.info('okbot ingest: {} data consumed from {}.'.format(consumed_num, file_name))
 

        logger.info('okbot ingest job finished. elapsed time: {} sec.'.format(time.time() - time_tic))


        now = timezone.now()
        try:
            job = Joblog.objects.get(name=jobid)
            job.finish_time = now
        except Exception as e:
            logger.error(e)
            logger.error('command okbot_ingest, fail to fetch job log. id: {}. create a new one'.format(jobid))
            # try:
            job = Joblog(name=jobid, start_time=now)
            # except Exception as e:
                # logger.error(e)
                # logger.error('command okbot_ingest, fail to create job log')
                # return
        finally:
            job.status = 'finished'
            job.save()


class Ingester(object):
    query_post_sql = '''
            SELECT * FROM ingest_app_post WHERE url IN %s;
    '''

    upsert_post_sql = '''
            INSERT INTO ingest_app_post(title, tokenized, grammar, tag, spider, url, 
                                        author, push, publish_date, last_update, update_count, allow_update)
            SELECT unnest( %(title)s ), unnest( %(tokenized)s ), unnest( %(grammar)s ), 
                   unnest( %(tag)s ), unnest( %(spider)s ), unnest( %(url)s ), unnest( %(author)s ), 
                   unnest( %(push)s ), unnest( %(publish_date)s ), unnest( %(last_update)s ), 
                   unnest( %(update_count)s ), unnest( %(allow_update)s )
            ON CONFLICT (url) DO 
            UPDATE SET 
                tokenized = EXCLUDED.tokenized,
                grammar = EXCLUDED.grammar,
                push = EXCLUDED.push,
                last_update = EXCLUDED.last_update,
                allow_update = EXCLUDED.allow_update,
                update_count = ingest_app_post.update_count + 1 
            WHERE ingest_app_post.allow_update = True;
    '''

    query_vocab_sql = '''
            SELECT * FROM ingest_app_vocabulary WHERE name IN %s;
    '''

    upsert_vocab_sql = '''
            INSERT INTO ingest_app_vocabulary(name, word, tokenizer, tag, doc_freq, stopword) 
            SELECT unnest( %(name)s ), unnest( %(word)s ), unnest( %(tokenizer)s ), unnest( %(tag)s ), unnest( %(doc_freq)s ), unnest( %(stopword)s )
            ON CONFLICT (name) DO NOTHING         
    '''

    upsert_vocab2post_sql = '''
            INSERT INTO ingest_app_vocabulary_post (vocabulary_id, post_id)
            SELECT unnest( %(vocabulary_id)s ), unnest( %(post_id)s )
            ON CONFLICT DO NOTHING
    '''

    query_vocab2post = '''
            SELECT vocabulary_id FROM ingest_app_vocabulary_post WHERE vocabulary_id IN %s;
    '''

    update_vocab_docfreq_sql = '''
            UPDATE ingest_app_vocabulary AS old SET doc_freq = new.doc_freq 
            FROM (SELECT unnest( %(id_)s ) as id, unnest( %(freq)s ) as doc_freq) as new  
            WHERE old.id = new.id;
    '''

    def __init__(self, spider_tag, tok_tag):
        self.spider_tag = spider_tag
        self.tok_tag = tok_tag

    def _query_all(self, sql_string, data=None):
        psql = PsqlQuery()
        fetched = list(psql.query(sql_string, data))
        schema = psql.schema
        return fetched, schema


    def query_vocab(self, name):
        qvocab, schema = self._query_all(self.query_vocab_sql, (tuple(name),))
        return qvocab, schema


    def query_post(self, url):
        qpost, schema = self._query_all(self.query_post_sql, (tuple(url),))
        aligned_post = [None] * len(url)
        for p in qpost:
            idx = url.index(p[schema['url']])
            aligned_post[idx] = p
        return aligned_post, schema

    def upsert_vocab_ignore_docfreq(self, batch_post):
        allpairs = [pair for post in batch_post for pair in post['title_tok']]
        name = list({'--+--'.join([pair.word, pair.flag, self.tok_tag]) for pair in allpairs})
        num = len(name)
        groups = [nm.split('--+--') for nm in name]
        word = [g[0] for g in groups]
        tag = [g[1] for g in groups]
        tokenizer = [g[2] for g in groups]
        doc_freq = [-1 for g in groups]
        stopword = [False for g in groups]
        psql = PsqlQuery()
        psql.upsert(self.upsert_vocab_sql, locals())

        return name

    def upsert_vocab2post(self, batch_post, vocab_name, post_url):
        qvocab, vschema = self.query_vocab(vocab_name)
        qpost, pschema = self.query_post(post_url)
        title_tok_name = [['--+--'.join([k.word, k.flag, self.tok_tag]) for k in p['title_tok']] for p in batch_post]

        vocab2post = []
        for vocab in qvocab:
            post_id_with_vocab = [p[pschema['id']] for idx, p in enumerate(qpost) if vocab[vschema['name']] in title_tok_name[idx]]
            vocab2post.append([(vocab[vschema['id']], pid) for pid in post_id_with_vocab])

        flatten_vocab2post = [tup for v2p in vocab2post for tup in v2p]

        vocabulary_id = [v2p[0] for v2p in flatten_vocab2post]
        post_id = [v2p[1] for v2p in flatten_vocab2post]

        psql = PsqlQuery()
        psql.upsert(self.upsert_vocab2post_sql, {'vocabulary_id': vocabulary_id, 'post_id': post_id})

        self._update_vocab_docfreq(vocabulary_id)


    def _update_vocab_docfreq(self, vocab_id):
        qvocab2post, schema = self._query_all(self.query_vocab2post, (tuple(vocab_id),))
        qvocab_id = [v2p[schema['vocabulary_id']] for v2p in qvocab2post]

        vocab_cnt = collections.Counter(qvocab_id)
        id_ = list(vocab_cnt.keys())
        freq = list(vocab_cnt.values())

        psql = PsqlQuery()
        psql.upsert(self.update_vocab_docfreq_sql, {'id_':id_, 'freq': freq})



    def upsert_post(self, batch_post):
        post_num = len(batch_post)
        
        title = [p['title'] for p in batch_post]
        tokenized = [p['title_vocab'] for p in batch_post]
        grammar = [p['title_grammar'] for p in batch_post]
        url = [p['url'] for p in batch_post]
        tag = [p['tag'] for p in batch_post]
        author = [p['author'] for p in batch_post]
        push = [p['push'] for p in batch_post]
        publish_date = [p['date'] for p in batch_post]
        spider = [self.spider_tag] * post_num
        last_update = [timezone.now()] * post_num
        update_count = [1] * post_num
        allow_update = [True] * post_num

        # qpost, schema = self.query_post(url)
        # for i, q in enumerate(qpost):
        #     if q:
        #         if len(q[schema['push']]) == len(push[i]):
        #             allow_update[i] = False
        try:
            psql = PsqlQuery()
            psql.upsert(self.upsert_post_sql, locals())
        except Exception as e:
            print(post_num)
            raise e

        return url
        

        



class CrawledJLParser(object):

    def __init__(self, jlpath, tokenizer):
        self.jlpath = jlpath
        self.tokenizer = tokenizer

    def batch_parse(self, batch_size=1000):
        with open(self.jlpath, 'r') as f:
            i = 0
            parsed = []
            for line in f:
                parsed.append(self._parse(line))
                i += 1
                if i >= batch_size:
                    i = 0
                    yield [ps for ps in parsed if ps]
                    parsed = []

            yield [ps for ps in parsed[:i] if ps]


    def _parse(self, line):
        try:
            post = json.loads(line)
            title_ = post['title']
            m = re.search('[\]|］]', title_)
            if m is None:
                title = title_.strip()
                tag = ''
            else:
                right_quote_idx = m.start()
                title = title_[right_quote_idx + 1 :].strip()
                tag = title_[1 : right_quote_idx].strip()

            title_tok, title_vocab, title_grammar = self.tokenizer.cut(title)
            return {
                'title': title,
                'tag': tag,
                'title_tok': title_tok,
                'title_vocab': ' '.join(title_vocab),
                'title_grammar': ' '.join(title_grammar),
                'url': post['url'],
                'author': post['author'],
                'date': timezone.datetime.strptime(post['date'], '%a %b %d %H:%M:%S %Y'),
                'push': '\n'.join(post['push']),
            }

        except Exception as e:
            logger.warning(e)
            logger.warning('command okbot_ingest, jsonline record faild to parse in, ignored. line: {}'.format(line.encode('utf-8').decode('unicode-escape')))
            return {}
