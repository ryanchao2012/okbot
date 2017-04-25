import time
import subprocess as sp
import os
import datetime
from utils import PsqlQuery

import logging
logger = logging.getLogger('okbot_cron_crawl+ingest')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setFormatter(chformatter)
logger.addHandler(ch)

SPIDER_OUTPUT_ROOT = 'crawl_app/spider/output'
SPIDER_UPDATE = 'python manage.py okbot_update_spider {}'
SPIDER_CRAWL = 'python manage.py okbot_crawl {}'
SPIDER_INGEST = 'python manage.py okbot_ingest --jlpath={} --tokenizer={}'

def __pipeline(spider_tag):
    sp.call(SPIDER_UPDATE.format(spider_tag).split())
    r = sp.check_output(SPIDER_CRAWL.format(spider_tag).split())
    filename = '{}.jl'.format(r.decode('utf-8').strip())
    complete_filepath = '{}/{}'.format(SPIDER_OUTPUT_ROOT, filename)
    if not os.path.isfile(complete_filepath):
        logger.error('okbot cronjob: crawled file: {} not found, cronjob abort.'.format(complete_filepath))
        return -1

    else:
        sp.call(SPIDER_INGEST.format(complete_filepath, 'jieba').split())
        logger.info('okbot cronjob: crawl/ingest: {} finished.'.format(filename))


if __name__ == '__main__':
    psql = PsqlQuery()
    allspiders = psql.query('SELECT tag, freq FROM crawl_app_spider;')
    schema = psql.schema
    for spider in allspiders:
        tag = spider[schema['tag']]
        freq = spider[schema['freq']]
        if freq > 0:
            delta = (datetime.datetime.today() - datetime.datetime(1970,1,1)).days
            #if delta % freq == 0:
            #    __pipeline(tag)
    __pipeline('Gossiping')

