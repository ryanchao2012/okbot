import json
from pygments import highlight, lexers, formatters
from scrapy.exceptions import DropItem
from utils import PsqlQuery
import numpy as np
import logging
logger = logging.getLogger('okbot_crawl')
#logger.setLevel(logging.INFO)
#ch = logging.StreamHandler()
#chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
#ch.setFormatter(chformatter)
#logger.addHandler(ch)



class PttSpiderPipeline(object):
    def __init__(self):
        self.title_len = []
        self.author_len = []
        self.date_len = []
        self.push_len = []
        self.url_len = []
        self.content_len = []
        self.drop_num = 0

    def process_item(self, item, spider):
        # TODO
        title_len = len(item['title'])
        author_len = len(item['author'])
        date_len = len(item['date'])
        url_len = len(item['url'])
        push_len = len(item['push'])
        content_len = len(item['content'])

        if not all([title_len, author_len, date_len, url_len, push_len, content_len]):
            self.drop_num += 1
            raise DropItem()
        else:
            self.title_len.append(title_len)
            self.author_len.append(author_len)
            self.date_len.append(date_len)
            self.url_len.append(url_len)
            self.push_len.append(push_len)
            self.content_len.append(content_len)
            return item
    def _calc_distribution(self, ls):
        arr = np.asarray(ls)
        return arr.mean(), arr.std()

    def _update_job_result(self, jobname, result):
        try:
            psql = PsqlQuery()
            update_joblog_result = '''
                UPDATE crawl_app_joblog
                SET result=%(result)s
                WHERE name = %(name)s;
            '''
            psql.upsert(update_joblog_result, {'name': jobname, 'result': result})

        except Exception as e:
            logger.error(e)
            pass

    def close_spider(self, spider):
        # f = lambda ls: sum(ls) / float(len(ls))
        name = spider.tag
        num = len(self.title_len)
        d = {}
        d['name'] = name
        d['item_num'] = '{:d}'.format(num)
        d['drop_num'] = '{:d}'.format(self.drop_num)
        d['title'] = 'mean: {:.1f}, std: {:.1f}'.format( *self._calc_distribution(self.title_len) )
        d['url'] = 'mean: {:.1f}, std: {:.1f}'.format( *self._calc_distribution(self.url_len) )
        d['author'] = 'mean: {:.1f}, std: {:.1f}'.format( *self._calc_distribution(self.author_len) )
        d['date'] = 'mean: {:.1f}, std: {:.1f}'.format( *self._calc_distribution(self.date_len) )
        d['push'] = 'mean: {:.1f}, std: {:.1f}'.format( *self._calc_distribution(self.push_len) )
        d['content'] = 'mean: {:.1f}, std: {:.1f}'.format( *self._calc_distribution(self.content_len) )
        
        formatted_json = json.dumps(d, indent = 4)
        colorful_json = highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())
        logger.info(colorful_json)
        spider.results = d

        self._update_job_result(spider.jobid, formatted_json)


