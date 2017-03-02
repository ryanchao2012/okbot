from django.core.management.base import BaseCommand, CommandError
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from django.utils import timezone
import time

import logging
logger = logging.getLogger('okbot_crawl')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setFormatter(chformatter)
logger.addHandler(ch)


from crawl_app.models import Spider, Joblog, Blacklist
from crawl_app.spider.pttspider import PttSpider


def _crawler_wrapper(f):
    def crawler_wrapper_(*args, **kwargs):
        settings = Settings()
        settings.set('LOG_LEVEL', 'WARNING')
        settings.set('USER_AGENT', 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)')
        settings.set('ITEM_PIPELINES', {'crawl_app.spider.pipelines.PttSpiderPipeline' : 400})
        settings.set('FEED_FORMAT', 'jsonlines')
        settings.set('DOWNLOAD_DELAY', 0.25)

        params = f(*args, **kwargs)

        settings.set('FEED_URI', 'crawl_app/spider/output/{}.jl'.format(params['jobid']))
        settings.set('LOG_FILE', 'crawl_app/spider/output/log-{}.txt'.format(params['jobid']))
        
        process = CrawlerProcess(settings)
        process.crawl(PttSpider, params['tag'], params['entry'], 
            blacklist=params['blacklist'],
            start_idx=params['start_idx'],
            end_idx=params['end_idx']
        )
        logger.info('okbot crawl job start. target: {}, from {} to {}.'.format(params['tag'], params['start_idx'], params['end_idx']))

        t = time.time()
        process.start()

        logger.info('okbot crawl job finished. elapsed time: {} sec.'.format(time.time() - t))
        now = timezone.now()
        try:
            job = Joblog.objects.get(name=params['jobid'])
        except Exception as e:
            logger.error(e)
            logger.error('command okbot_crawl, fail to fetch job log. id: {}. create a new one'.format(params['jobid']))
            try:
                
                job = Joblog(name=params['jobid'], start_time=now, finish_time=now, status='finished')
            except Exception as e:
                logger.error(e)
                logger.error('command okbot_crawl, fail to create job log')
                return

        job.finish_time = now
        job.status = 'finished'
        job.save()



    return crawler_wrapper_



class Command(BaseCommand):
    help = '''
           start crawling ptt by given <spider-tag>.
           ex: python manage.py okbot_crawl Gossiping
           '''
    def add_arguments(self, parser):
        parser.add_argument('spider_tag', nargs=1, type=str)

    def handle(self, *args, **options):
        tag = options['spider_tag'][0]
        try:
            spider = Spider.objects.get(tag=tag)
        except Exception as e:
            logger.error(e)
            logger.error('command: okbot_crawl,  spider: <{}> is not found'.format(tag))
            return -1
        if spider.status != 'pass':
            logger.warning('command: okbot_crawl, spider is not in "pass" status, please update spider first.')
            return

        now = timezone.now()
        jobid = '{}.{}.{}.{}'.format(tag.lower(), spider.start, spider.end, now.strftime('%Y-%m-%d-%H-%M-%S'))
        try:
            Joblog(name=jobid, start_time=now, status='running').save()
        except Exception as e:
            pass
            logger.error(e)
            logger.error('command okbot_crawl, fail to create job log')
        self._crawl(spider, jobid)

        try:
            job = Joblog.objects.get(name=jobid)
            if job.status != 'finished':
                job.status = 'terminated'
                job.save()
        except Exception as e:
            logger.error(e)
            logger.error('command okbot_crawl, fail to final check job log. id: {}.'.format(jobid))


    @_crawler_wrapper
    def _crawl(self, spider, jobid, **kwargs):
        blacklist = {}

        btype = ['title', 'push', 'author']
        btype = [t[1] for t in Blacklist.BLIST_TYPE_CHOICES[1:]]
        blist = spider.blacklist.all()
        for b in blist:
            type_ = Blacklist.BLIST_TYPE_CHOICES[b.btype][1]
            if type_ in btype:
                if type_ in blacklist:
                    blacklist[type_].extend([p.strip() for p in b.phrases.split(',')])
                else:
                    blacklist[type_] = [p.strip() for p in b.phrases.split(',')]

        newest_idx = spider.newest
        start_idx = spider.start
        if start_idx >= newest_idx:
            start_idx = 1 + int(newest_idx * 0.9)
        end_idx = spider.end
        if end_idx >= newest_idx:
            end_idx = 1 + int(newest_idx * 0.9)

        if start_idx >= end_idx:
            end_idx = start_idx
        return {
            'jobid': jobid,
            'tag': spider.tag,
            'entry': spider.entry,
            'start_idx': start_idx,
            'end_idx': end_idx,
            'blacklist': blacklist
        }

