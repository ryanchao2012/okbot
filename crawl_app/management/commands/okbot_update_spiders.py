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
    help = 'update the newest index of all spiders'

    def handle(self, *args, **options):
        lastlist_url_xpath = '//div[@class="btn-group btn-group-paging"]/a[2]/@href'
        re_pattern = r'index(\d+)\.html'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36'
        }
        cookies= {'over18': '1'}

        spider = Spider.objects.all()
        newest = []
        for sp in spider:
            entry = sp.entry.format(index='')
            try:
                response = requests.get(entry, headers=headers, cookies=cookies)
                tree = html.fromstring(response.text)
                last_index_info = tree.xpath(lastlist_url_xpath)[0]
                last_index = int(re.search(re_pattern, last_index_info).group(1))
                newest.append(last_index)
            except Exception as e:
                newest.append(-1)
                logger.error(e)


        with transaction.atomic():
            for sp, new_idx in zip(spider, newest):
                sp.newest = new_idx
                if new_idx > 0:
                    sp.status = 'pass'
                    
                else:
                    sp.status = 'debug'
                sp.save()

        logger.info('command: okbot_update, all jobs finished.')

