# -*- coding: utf-8 -*-
from django.core.management.base import BaseCommand
from django.utils import timezone
import time

from utils import PsqlQuery

import logging
logger = logging.getLogger('okbot_outgest')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setFormatter(chformatter)
logger.addHandler(ch)


class Command(BaseCommand):
    help = '''
           Delete the old records from database.
           ex: python manage.py okbot_outgest --date <year-month-date>
           '''

    def add_arguments(self, parser):
        parser.add_argument('--date', nargs=1, type=str)

    def handle(self, *args, **options):
        fromdate_ = options['date'][0]
        try:
            fromdate = timezone.datetime.strptime(fromdate_, '%Y-%m-%d')
        except Exception as e:
            date_ = '2005-01-01'
            logger.warning(e)
            logger.warning('Fail to parse date, the correct format is: <year>-<month>-<date>. Replace the date to {}.'.format(date_))
            fromdate = timezone.datetime.strptime(date_, '%Y-%m-%d')

        time_tic = time.time()
        logger.info('okbot outgest job start.')

        outgester = Outgester(fromdate.strftime('%Y-%m-%d'))
        delete_num = 0
        for batch_post, pschema in outgester.query_oldpost_batch():
            if len(batch_post) > 0:
                outgester.clean_oldpost(batch_post, pschema)

                delete_num += len(batch_post)
            logger.info('okbot outgest: {} data deleted.'.format(delete_num))

        logger.info('okbot outgest job finished. elapsed time: {:.2f} sec.'.format(time.time() - time_tic))


class Outgester(object):
    delete_vocab2post_sql = '''
            DELETE FROM ingest_app_vocabulary_post WHERE id in %s;
    '''

    delete_post_sql = '''
            DELETE FROM ingest_app_post WHERE id in %s;
    '''

    query_post_sql = '''
            SELECT id, title, publish_date, url
            FROM ingest_app_post WHERE publish_date < TIMESTAMP %s;
    '''

    query_vocab2post_sql_by_post = '''
            SELECT * FROM ingest_app_vocabulary_post WHERE post_id IN %s;
    '''

    query_vocab2post_sql_by_vocab = '''
            SELECT * FROM ingest_app_vocabulary_post WHERE vocabulary_id IN %s;
    '''

    update_vocab_docfreq_sql = '''
            UPDATE ingest_app_vocabulary AS old SET doc_freq = new.doc_freq
            FROM (SELECT unnest( %(id_)s ) as id, unnest( %(freq)s ) as doc_freq) as new
            WHERE old.id = new.id;
    '''

    def __init__(self, fromdate):
        self.fromdate = fromdate

    def _query_all(self, sql_string, data=None):
        psql = PsqlQuery()
        fetched = list(psql.query(sql_string, data))
        schema = psql.schema
        return fetched, schema

    def clean_oldpost(self, batch_post, pschema):
        post_id = [p[pschema['id']] for p in batch_post]

        vocab2post, v2pschema = self._query_all(
            self.query_vocab2post_sql_by_post, (tuple(post_id),)
        )

        v2p_id = [v2p[v2pschema['id']] for v2p in vocab2post]
        vocab_id = list({v2p[v2pschema['vocabulary_id']] for v2p in vocab2post})

        psql = PsqlQuery()
        psql.delete(self.delete_vocab2post_sql, (tuple(v2p_id),))
        psql.delete(self.delete_post_sql, (tuple(post_id),))

        self._update_vocab_docfreq(vocab_id)

    def _update_vocab_docfreq(self, vocab_id):
        qvocab2post, schema = self._query_all(self.query_vocab2post_sql_by_vocab, (tuple(vocab_id),))
        qvocab_id = [v2p[schema['vocabulary_id']] for v2p in qvocab2post]

        vocab_cnt = collections.Counter(qvocab_id)
        for vid in vocab_id:
            if vid not in qvocab_id:
                vocab_cnt[vid] = 0
        id_ = list(vocab_cnt.keys())
        freq = list(vocab_cnt.values())

        psql = PsqlQuery()
        psql.upsert(self.update_vocab_docfreq_sql, {'id_': id_, 'freq': freq})

    def query_oldpost_batch(self, batch_size=1000):
        psql = PsqlQuery()
        fetched = psql.query(self.query_post_sql, (self.fromdate,))
        schema = psql.schema

        batch, i = [], 0
        for qpost in fetched:
            batch.append(qpost)
            i += 1
            if i >= batch_size:
                i = 0
                yield batch, schema
                batch = []

        yield batch, schema

# In [50]: p = psql._query(query_='''SELECT title, publish_date, ur
#     ...: l FROM ingest_app_post WHERE publish_date > TIMESTAMP '2
#     ...: 017-04-12 00:00:00'; ''')
