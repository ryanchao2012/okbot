from items import PttSpiderItem
from pipelines import PttSpiderPipeline
from pttspider import PttSpider

from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from time import time
import os
import sys, getopt



def main(argv = None):

    settings = Settings()
    settings.set('FEED_URI', 'output.jl')
    settings.set('LOG_FILE', 'log.txt')
    settings.set('FEED_FORMAT', 'jsonlines')
    settings.set('LOG_LEVEL', 'WARNING')
    settings.set('USER_AGENT', 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)')
    settings.set('ITEM_PIPELINES', {'pipelines.PttSpiderPipeline' : 400})
    settings.set('DOWNLOAD_DELAY', 0.25)

    tag = 'gossiping'
    entry_url = 'https://www.ptt.cc/bbs/gossiping/index{index}.html'

    # url_xpath = '//div[@class="title"]/a/@href'
    # title_xpath = '//div[@id="main-content"]/div[@class="article-metaline"][2]/span[@class="article-meta-value"]'
    # author_xpath = '//div[@id="main-content"]/div[@class="article-metaline"][1]/span[@class="article-meta-value"]'
    # date_xpath = '//div[@id="main-content"]/div[@class="article-metaline"][3]/span[@class="article-meta-value"]'
    # push_xpath = '//div[@class="push"]/span[@class="f3 push-content"]'
    # author_pattern = 'By{}'
    # date_pattern = '{}·'
    t = time()

    process = CrawlerProcess(settings)
    process.crawl(PttSpider, tag, entry_url, 
        blacklist={
            'title': ['公告', 'Re:', 'Fw:'], 
            'push': ['推', '噓', '樓', '站內', '八卦', '版', '板', '刪', '站內', '蓋', '篇', '原po']
        },
        start_idx=20181,
        end_idx=20181
    )

    process.start()

    print('good')
    print(time() - t)





if __name__ == '__main__':
    main()
