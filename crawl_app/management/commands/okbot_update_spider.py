from django.core.management.base import BaseCommand, CommandError
from crawl_app.models import Spider
from django.db import transaction
from lxml import html
import requests
import time
import re

import logging
logger = logging.getLogger('okbot_update')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setLevel(logging.INFO)
ch.setFormatter(chformatter)
logger.addHandler(ch)

class Command(BaseCommand):
    help = '''
    	Update the newest index and crawl range of spider specified by spider_tag
    	ex: python manage.py okbot_update_spider <spider-tag>
    '''

    def add_arguments(self, parser):
        parser.add_argument('spider_tag', nargs=1, type=str)

    def handle(self, *args, **options):
        tag = options['spider_tag'][0]
        spider = Spider.objects.get(tag=tag)

        lastlist_url_xpath = '//div[@class="btn-group btn-group-paging"]/a[2]/@href'
        re_pattern = r'index(\d+)\.html'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36'
        }
        cookies= {'over18': '1'}

        
        # newest = []
        # for sp in spider:
        entry = spider.entry.format(index='')
        try:
            response = requests.get(entry, headers=headers, cookies=cookies)
            tree = html.fromstring(response.text)
            last_index_info = tree.xpath(lastlist_url_xpath)[0]
            last_index = int(re.search(re_pattern, last_index_info).group(1))
            if last_index > 0:
                spider.newest = last_index
                spider.end = spider.newest - max(1, spider.offset)
                spider.start = spider.end - max(1, spider.page)
                spider.status = 'pass'
            else:
                spider.status = 'debug'

        except Exception as e:
            spider.newest = -1
            sp.status = 'debug'
            logger.error(e)

        finally:
            spider.save()


        logger.info('command: okbot_update: spider <{}> finished.'.format(tag))

