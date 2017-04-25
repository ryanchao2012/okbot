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
    max_push_num = 30

    start_idx = -1
    end_idx = -1

    reReplacePat = [
        (re.compile(r'<[^>]*>'), r''), 
        (re.compile(r'(<br\s?/>)+'), r'\n'),
        (re.compile(r'(\s*[\r|\n|\t]+\s*)+'), r'\n'),
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
    audience_xpath = '//div[@class="push"]/span[@class="f3 hl push-userid"]'
    push_xpath = '//div[@class="push"]/span[@class="f3 push-content"]'
    content_xpath = '//div[@id="main-content"]'
    content_end = r'\n?-+\n'

    def __init__(self, tag, entry, jobid, *args, **kwargs):
        super(PttSpider, self).__init__(*args, **kwargs)
        self.tag = tag
        self.entry = entry
        self.jobid = jobid
        self.results = None

        for k, v in kwargs.items():
            if self.__getattribute__(k) is not None:
                self.__setattr__(k, v)


    def start_requests(self):
        if self.start_idx <= 0 or self.end_idx <= 0:
            yield scrapy.Request(self.entry.format(index=''), callback=self.parse, headers=self.headers, cookies={'over18': 1})
        else:
            if self.start_idx >= self.end_idx:
                yield scrapy.Request(self.entry.format(index=self.start_idx), callback=self.parse, headers=self.headers, cookies={'over18': 1})
            else:
                for idx in range(self.start_idx, self.end_idx + 1):
                    yield scrapy.Request(self.entry.format(index=idx), callback=self.parse, headers=self.headers, cookies={'over18': 1})
        

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


    def parse_dir_contents(self, response):
        if 'item' not in response.meta: item = PttSpiderItem()
        else: item = response.meta['item']

        # title:
        item['title'] = ''  
        if self.title_xpath: 
            title = self.extract_first(response, self.title_xpath)
            if 'title' in self.blacklist:
                btitle = []
                btitle.extend(list(self.blacklist.get('title')))
                if any([t in title for t in btitle]):
                    title = ''
            title_ = re.sub(r'\[[^\]]*\]', '', title).strip()
            title_ = re.sub(r'［[^］]*］', '', title_).strip()
            if len(title_) == 0:
                title = ''
            item['title'] = title
                      

        # author:
        item['author'] = ''
        if self.author_xpath: 
            author = self.extract_first(response, self.author_xpath)
            # author = re.sub(r'\([^\)]*\)', '', author).strip()
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
        if self.push_xpath and self.audience_xpath: 
            push = [p.strip() for p in self.extract_all(response, self.push_xpath)]
            audi = self.extract_all(response, self.audience_xpath)
            push_length = len(push)
            if len(audi) == push_length:
                black_idx = []
                if 'push' in self.blacklist:
                    bpush = '|'.join(self.blacklist.get('push'))
                    black_idx.extend([i for i, p in enumerate(push) if re.search(bpush, p)])

                if 'audience' in self.blacklist:
                    baudi = '|'.join(self.blacklist.get('audience'))
                    black_idx.extend([i for i, a in enumerate(audi) if re.search(baudi, a)])

                black_idx.extend([i for i, p in enumerate(push) if len(p) < 2])
                black_idx = set(black_idx)
                audi_push = [''.join([audi[i], push[i]]) for i in range(push_length) if i not in black_idx]
                item['push'] = audi_push[:self.max_push_num]


        item['content'] = ''
        if self.content_xpath and len(item['date']) > 0:
            dirty_content = response.xpath(self.content_xpath).extract_first(default='')
            mstart = re.search(item['date'], dirty_content)
            mend = re.search(self.content_end, dirty_content)
            if mstart and mend:
                istart = mstart.end()
                iend = mend.start()
                if iend > istart:
                    item['content'] = self.__clean_html(dirty_content[istart : iend])

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

