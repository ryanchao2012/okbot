# -*- coding: utf-8 -*-
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
import jieba.posseg as pseg
import collections
import json
import time

from ingest_app.models import Joblog 
import psycopg2

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
           ex: python manage.py okbot_ingest <jsonline-file>
           '''
    idx_post = {
        'pk': 0,
        'title': 1,
        'tag': 2,
        'spider': 3,
        'url': 4,
        'author': 5,
        'push': 6,
        'publish_date': 7,
        'last_update': 8,
        'update_count': 9,
        'allow_update': 10
    }

    idx_vocab = {
        'pk': 0,
        'word': 1,
        'tokenizer': 2,
        'tag': 3,
        'excluded': 4,
        'doc_freq': 5,
        'name': 6
    }

    idx_vocab2post = {
        'pk': 0,
        'vocabulary_pk': 1,
        'post_pk': 2,
    }

    idx_grammar = {
        'pk': 0,
        'sent_tag': 1,
        'tokenizer': 2,
        'doc_freq': 3,
        'name': 4
    }


    def add_arguments(self, parser):
        parser.add_argument('--jlpath', nargs=1, type=str)
        parser.add_argument('--tokenizer', nargs=1, type=str)


    def _batch_input(self, fpath, batch_size=1000):
        
        with open(fpath, 'r') as f:
            buffer_ = [None] * batch_size
            i = 0
            for line in f:
                buffer_[i] = line
                i += 1
                if i >= batch_size:
                    parsed = [self._parse(l_) for l_ in buffer_[:i]]
                    yield [ps for ps in parsed if ps]
                    i = 0
                    buffer_ = [None] * batch_size

            yield [self._parse(l_) for l_ in buffer_[:i]]
         

    def _parse(self, line):
        try:
            post = json.loads(line)
            title_ = post['title']
            right_quote_idx = title_.find(']')
            if right_quote_idx < 0:
                right_quote_idx = title_.find('］')


            if right_quote_idx < 0:
                title = title_.strip()
                tag = ''
            else:
                title = title_[right_quote_idx + 1 :].strip()
                tag = title_[1 : right_quote_idx].strip()

            title_tok, title_grammar = self.tokenizer.cut(title)
            return {
                'title': title,
                'tag': tag,
                'title_tok': title_tok,
                'title_grammar': title_grammar,
                'url': post['url'],
                'author': post['author'],
                'date': timezone.datetime.strptime(post['date'], '%a %b %d %H:%M:%S %Y'),
                'push': '\n'.join(post['push']),
            }

        except Exception as e:
            logger.warning(e)
            logger.warning('command okbot_ingest, jsonline record faild to parse in, ignored. line: {}'.format(line.encode('utf-8').decode('unicode-escape')))
            return {}
    
    
    def _query_post(self, batch_post):
        url = [p['url'] for p in batch_post]
        self.cur.execute("SELECT * FROM ingest_app_post WHERE url IN %s;", (tuple(url),))
        post_query = self.cur.fetchall()
        post_query_ = [None] * len(post_query)
        for q in post_query:
            post_query_[url.index(q[self.idx_post['url']])] = q
        # batch_post_idx = [url.index(q[self.idx_post['url']]) for q in post_query]
        return post_query_
    
    def _upsert_post(self, batch_post):
        n_ = len(batch_post)
        title = [p['title'] for p in batch_post]
        url = [p['url'] for p in batch_post]
        tag = [p['tag'] for p in batch_post]
        spider = [self.spider_tag] * n_
        author = [p['author'] for p in batch_post]
        push = [p['push'] for p in batch_post]
        publish_date = [p['date'] for p in batch_post]
        last_update = [timezone.now()] * n_
        update_count = [1] * n_
        allow_update = [True] * n_

        post_query = self._query_post(batch_post)

        
        for i_, q in enumerate(post_query):
            if len(q[self.idx_post['push']]) == len(push[i_]):
                allow_update[i_] = False

        SQL = '''
            INSERT INTO ingest_app_post(title, tag, spider, url, author, push, publish_date, last_update, update_count, allow_update) 
            SELECT unnest( %(title)s ), unnest( %(tag)s ), unnest( %(spider)s ), unnest( %(url)s ), unnest( %(author)s ),
                   unnest( %(push)s ), unnest( %(publish_date)s ), unnest( %(last_update)s ), unnest( %(update_count)s ), unnest( %(allow_update)s )
            ON CONFLICT (url) DO 
            UPDATE SET 
            update_count = ingest_app_post.update_count + 1, 
            push = EXCLUDED.push,
            last_update = EXCLUDED.last_update,
            allow_update = EXCLUDED.allow_update
            WHERE ingest_app_post.allow_update = True;
        '''
        self.cur.execute(SQL, locals())
        self.conn.commit()

    def _update_vocab_df(self, vocabulary_id):
        self.cur.execute("SELECT vocabulary_id FROM ingest_app_vocabulary_post WHERE vocabulary_id IN %s;", (tuple(vocabulary_id),))
        vocab2post_tuple = self.cur.fetchall()
        vocab_pk = [v2p[0] for v2p in vocab2post_tuple]

        vocab_cnt = collections.Counter(vocab_pk)
        id_ = list(vocab_cnt.keys())
        freq = list(vocab_cnt.values())

        SQL = '''
            UPDATE ingest_app_vocabulary AS old SET doc_freq = new.doc_freq 
            FROM (SELECT unnest( %(id_)s ) as id, unnest( %(freq)s ) as doc_freq) as new  
            WHERE old.id = new.id;
        '''
        self.cur.execute(SQL, locals())
        self.conn.commit()

    def _upsert_grammar_ignore_df(self, batch_post, sent_tag):
        n_ = len(sent_tag)
        tokenizer = [self.tok_tag] * n_
        doc_freq = [-1] * n_
        name = [':'.join([self.tok_tag, s]) for s in sent_tag]
        SQL = '''
            INSERT INTO ingest_app_grammar(name, sent_tag, tokenizer, doc_freq) 
            SELECT unnest( %(name)s ), unnest( %(sent_tag)s ), unnest( %(tokenizer)s ), unnest( %(doc_freq)s )
            ON CONFLICT (name) DO NOTHING  
        '''
        self.cur.execute(SQL, locals())
        self.conn.commit()

    def _upsert_grammar2post(self, batch_post, sent_tag):
        name = [':'.join([self.tok_tag, s]) for s in sent_tag]
        self.cur.execute("SELECT * FROM ingest_app_grammar WHERE name IN %s;", (tuple(name),))
        grammar_guery = self.cur.fetchall()
        grammar_sent_tag = [g[self.idx_grammar['sent_tag']] for g in grammar_guery]
        post_query = self._query_post(batch_post)
        post_sent_tag = [p['title_grammar'] for p in batch_post]

        grammar2post = [(grammar_guery[grammar_sent_tag.index(post_sent_tag[i_])][self.idx_grammar['pk']] , q[self.idx_post['pk']]) for i_, q in enumerate(post_query)]
        
        grammar_id = [g2p[0] for g2p in grammar2post]
        post_id = [g2p[1] for g2p in grammar2post]
        SQL = '''
        INSERT INTO ingest_app_grammar_post (grammar_id, post_id)
        SELECT unnest( %(grammar_id)s ), unnest( %(post_id)s )
        ON CONFLICT DO NOTHING         
        '''
        self.cur.execute(SQL, locals())
        self.conn.commit()
        
        self._update_grammar_df(grammar_id)
    
    def _update_grammar_df(self, grammar_id):
        self.cur.execute("SELECT grammar_id FROM ingest_app_grammar_post WHERE grammar_id IN %s;", (tuple(grammar_id),))
        grammar2post_tuple = self.cur.fetchall()
        grammar_pk = [g2p[0] for g2p in grammar2post_tuple]

        grammar_cnt = collections.Counter(grammar_pk)
        id_ = list(grammar_cnt.keys())
        freq = list(grammar_cnt.values())

        SQL = '''
            UPDATE ingest_app_grammar AS old SET doc_freq = new.doc_freq 
            FROM (SELECT unnest( %(id_)s ) as id, unnest( %(freq)s ) as doc_freq) as new  
            WHERE old.id = new.id;
        '''
        self.cur.execute(SQL, locals())
        self.conn.commit()

    def _upsert_vocab_ignore_df(self, batch_post, tok_name):
        name = tok_name
        n_ = len(name)
        groups = [nm.split('--+--') for nm in name]

        word = [g[0] for g in groups]
        tag = [g[1] for g in groups]
        tokenizer = [g[2] for g in groups]
        excluded = [False] * n_
        doc_freq = [-1] * n_
        
        SQL = '''
            INSERT INTO ingest_app_vocabulary(name, word, tokenizer, tag, excluded, doc_freq) 
            SELECT unnest( %(name)s ), unnest( %(word)s ), unnest( %(tokenizer)s ), unnest( %(tag)s ), unnest( %(excluded)s ), unnest( %(doc_freq)s )
            ON CONFLICT (name) DO NOTHING         
        '''

        self.cur.execute(SQL, locals())
        self.conn.commit()

    
    def _upsert_vocab2post(self, batch_post, tok_name):
        self.cur.execute("SELECT * FROM ingest_app_vocabulary WHERE name IN %s;", (tuple(tok_name),))
        vocab_query = self.cur.fetchall()
        post_query = self._query_post(batch_post)

        title_tok_name = [['--+--'.join([k.word, k.flag, self.tok_tag]) for k in p['title_tok']] for p in batch_post]

        vocab2post = [None] * len(vocab_query)
        for iv_, vocab in enumerate(vocab_query):
            post_pk_with_vocab = [p[self.idx_post['pk']] for j_, p in enumerate(post_query) if vocab[self.idx_vocab['name']] in title_tok_name[j_]]
            
            vocab2post[iv_] = [(vocab[self.idx_vocab['pk']], ppk) for ppk in post_pk_with_vocab]


        flatten_vocab2post = [tup for v2p in vocab2post for tup in v2p]

        vocabulary_id = [v2p[0] for v2p in flatten_vocab2post]
        post_id = [v2p[1] for v2p in flatten_vocab2post]
        SQL = '''
        INSERT INTO ingest_app_vocabulary_post (vocabulary_id, post_id)
        SELECT unnest( %(vocabulary_id)s ), unnest( %(post_id)s )
        ON CONFLICT DO NOTHING         
        '''

        self.cur.execute(SQL, locals())
        self.conn.commit()

        self._update_vocab_df(vocabulary_id)


    def handle(self, *args, **options):
        jlpath = options['jlpath'][0]
        self.tok_tag = options['tokenizer'][0]
        
        file_name = jlpath.split('/')[-1]
        self.spider_tag = file_name[: file_name.find('.')]

        self.tokenizer = Tokenizer(self.tok_tag)

        time_tic = time.time()
        logger.info('okbot ingest job start. source: {}'.format(file_name))
        now = timezone.now()
        jobid =  '.'.join(file_name.split('.')[:3] + [now.strftime('%Y-%m-%d-%H-%M-%S')])
        try:
            Joblog(name=jobid, start_time=now, status='running').save()
        except Exception as e:
            pass
            logger.error(e)
            logger.error('command okbot_ingest, fail to create job log')

        self.conn = psycopg2.connect(database='tripper', user='tripper')
        self.cur = self.conn.cursor()


        for batch_post in self._batch_input(jlpath):
            self._upsert_post(batch_post)
            title_tok = [p['title_tok'] for p in batch_post]
            
            vocabs = [item for sublist in title_tok for item in sublist]
            tok_name = list({'--+--'.join([v.word, v.flag, self.tok_tag]) for v in vocabs})
            self._upsert_vocab_ignore_df(batch_post, tok_name)
            self._upsert_vocab2post(batch_post, tok_name)
            
            post_sent_tag = [p['title_grammar'] for p in batch_post]
            sent_tag = list({s for s in post_sent_tag})
            self._upsert_grammar_ignore_df(batch_post, sent_tag)
            self._upsert_grammar2post(batch_post, sent_tag)

        self.cur.close()
        self.conn.close()
        logger.info('okbot ingest job finished. elapsed time: {} sec.'.format(time.time() - time_tic))


        now = timezone.now()
        try:
            job = Joblog.objects.get(name=jobid)
        except Exception as e:
            logger.error(e)
            logger.error('command okbot_ingest, fail to fetch job log. id: {}. create a new one'.format(jobid))
            try:
                
                job = Joblog(name=jobid, start_time=now, finish_time=now, status='finished')
            except Exception as e:
                logger.error(e)
                logger.error('command okbot_ingest, fail to create job log')
                return

        job.finish_time = now
        job.status = 'finished'
        job.save()



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
            pairs = list(pseg.cut(sentence))
            return {e for e in pairs if len(e.word.strip()) > 0}, '-'.join([p.flag for p in pairs])
        else:
            return {}



