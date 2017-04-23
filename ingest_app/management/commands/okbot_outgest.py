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
        now = timezone.now()

        # jobid =  '.'.join(file_name.split('.')[:3] + [now.strftime('%Y-%m-%d-%H-%M-%S')])
        # Joblog(name=jobid, start_time=now, status='running').save()

        outgester = Outgester()
        outgester.query_oldpost((fromdate.strftime('%Y-%m-%d'),))
        #consumed_num = 0
        #for batch_post in jlparser.batch_parse():
        #    if len(batch_post) > 0:
        #        post_url = ingester.upsert_post(batch_post)
        #        vocab_name = ingester.upsert_vocab_ignore_docfreq(batch_post)
        #        ingester.upsert_vocab2post(batch_post, vocab_name, post_url)
        #        consumed_num += len(batch_post)
        #        logger.info('okbot ingest: {} data consumed from {}.'.format(consumed_num, file_name))

        logger.info('okbot outgest job finished. elapsed time: {:.2f} sec.'.format(time.time() - time_tic))



class Outgester(object):
    delete_vocab2post_sql = '''
            DELETE FROM ingest_app_vocabulary_post WHERE id in %s;
    '''

    query_post_sql = '''
            SELECT id, title, publish_date, url 
            FROM ingest_app_post WHERE publish_date < TIMESTAMP %s;
    '''

    def __init__(self):
        pass

    def _query_all(self, sql_string, data=None):
        psql = PsqlQuery()
        fetched = list(psql.query(sql_string, data))
        schema = psql.schema
        return fetched, schema

    def query_oldpost(self, fromdate):
        oldpost, schema = self._query_all(self.query_post_sql, fromdate)
        print(len(oldpost))
        print(schema)
        _ = [print(p[schema['title']], p[schema['url']]) for p in oldpost]
        
        return oldpost, schema



# In [50]: p = psql._query(query_='''SELECT title, publish_date, ur
#     ...: l FROM ingest_app_post WHERE publish_date > TIMESTAMP '2
#     ...: 017-04-12 00:00:00'; ''')
