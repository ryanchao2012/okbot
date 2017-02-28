import scrapy
import re
from .items import PttSpiderItem
import math

try:
    # Python 2.6-2.7 
    from HTMLParser import HTMLParser
except ImportError:
    # Python 3
    from html.parser import HTMLParser
html = HTMLParser()



class PttSpider(scrapy.Spider):
    name = 'ptt'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36'
    }

    start_idx = -1
    end_idx = -1

    reReplacePat = [
        (re.compile(r'<[^>]*>'), r''), #(re.compile(r'(<br\s?/>)+'), r'\n'),
        # (re.compile(r'\[[^\]]*\]'), r''),
        # (re.compile(r'\([^\)]*\)'), r''),
        # (re.compile(r'［[^］]*］'), r''),
        # (re.compile(r'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)'), r''),
    ]

    blacklist = {}
    # reTokens = re.compile(r'([\.\^\$\|\*\+\?\\\[\]\(\)])')


    lastlist_xpath = '//div[@class="btn-group btn-group-paging"]/a[2]'


    # article_xpath = ''
    url_xpath = '//div[@class="title"]/a'
    author_xpath = '//div[@id="main-content"]/div[@class="article-metaline"][1]/span[@class="article-meta-value"]'
    title_xpath = '//div[@id="main-content"]/div[@class="article-metaline"][2]/span[@class="article-meta-value"]'
    date_xpath = '//div[@id="main-content"]/div[@class="article-metaline"][3]/span[@class="article-meta-value"]'
    push_xpath = '//div[@class="push"]/span[@class="f3 push-content"]'

    def __init__(self, tag, entry, *args, **kwargs):
        super(PttSpider, self).__init__(*args, **kwargs)
        self.tag = tag
        self.entry = entry
        self.results = None

        for k, v in kwargs.items():
            if self.__getattribute__(k) is not None:
                self.__setattr__(k, v)

        # if 'blacklist' in kwargs:
        #     blist = kwargs.get('blacklist')
        #     if isinstance(blist, dict):
        #         self.blacklist = blist

        # if 'url_xpath' in kwargs:
        #     xpath = kwargs.get('url_xpath')
        #     if isinstance(xpath, str): 
        #         self.url_xpath = xpath


    def start_requests(self):
        if self.start_idx <= 0 or self.end_idx <= 0:
            yield scrapy.Request(self.entry.format(index=''), callback=self.parse, headers=self.headers, cookies={'over18': 1})
        else:
            if self.start_idx >= self.end_idx:
                yield scrapy.Request(self.entry.format(index=self.start_idx), callback=self.parse, headers=self.headers, cookies={'over18': 1})
            else:
                for idx in range(self.start_idx, self.end_idx + 1):
                    yield scrapy.Request(self.entry.format(index=idx), callback=self.parse, headers=self.headers, cookies={'over18': 1})

        # if self.num_page:
        #     for i in range(self.num_page):
        #         url = self.entry + str(i + 1)
        #         yield self.make_requests_from_url(url)
        # else:
        

    def parse(self, response):
        btitle = []
        if 'title' in self.blacklist:
            btitle.extend(list(self.blacklist.get('title')))
        for sel_url in response.xpath(self.url_xpath):
            cxt = sel_url.extract()
            if any([t in cxt for t in btitle]):
                continue
            item = PttSpiderItem()
            href = sel_url.xpath('@href').extract_first()
            url = response.urljoin(href.strip())
            item['url'] = url
            request = scrapy.Request(url, callback=self.parse_dir_contents, headers=self.headers, cookies={'over18': 1})
            request.meta['item'] = item
            yield request
        # return PttSpider.parse(response, self.url_xpath, self.parse_dir_contents)

    def parse_dir_contents(self, response):
        if 'item' not in response.meta: item = PttSpiderItem()
        else: item = response.meta['item']

        # title:
        item['title'] = ''  
        if self.title_xpath: 
            title = self.extract_first(response, self.title_xpath)
            title = re.sub(r'\[[^\]]*\]', '', title).strip()
            title = re.sub(r'［[^］]*］', '', title).strip()
            item['title'] = title
                      

        # author:
        item['author'] = ''
        if self.author_xpath: 
            author = self.extract_first(response, self.author_xpath)
            author = re.sub(r'\([^\)]*\)', '', author).strip()
            if 'author' in self.blacklist:
                if author not in self.blacklist.get('author'):
                    item['author'] = author
            else:
                item['author'] = author

        # date:
        item['date'] = ''
        if self.date_xpath: 
            item['date'] = self.extract_first(response, self.date_xpath)
            

        # push:
        item['push'] = []
        if self.push_xpath: 
            push = self.extract_all(response, self.push_xpath)
            push = [p[1:].strip() for p in push]
            if 'push' in self.blacklist:
                bpush = '|'.join(self.blacklist.get('push'))
                push = [p for p in push if not re.search(bpush, p)]
            # item['push'] = sorted(push, key=len, reverse=True)[:30]
            push = [p for p in push if len(p) > 1]
            item['push'] = push[:30]
        return item

    def extract_first(self, response, pattern):
        content_string = response.xpath(pattern).extract_first(default='').strip() 
        return self.__clean_html(content_string)


    def extract_all(self, response, pattern):
        content_list = response.xpath(pattern).extract()
        return [self.__clean_html(c) for c in content_list] 

    def __clean_html(self, raw_html):
        for pat, rep in self.reReplacePat:
            raw_html = re.sub(pat, rep, raw_html).strip()
        clean_content = html.unescape(raw_html)
        return clean_content

