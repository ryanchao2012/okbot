import json
from pygments import highlight, lexers, formatters
from scrapy.exceptions import DropItem

class PttSpiderPipeline(object):
    def __init__(self):
        self.title_len = []
        self.author_len = []
        self.date_len = []
        self.push_len = []
        self.url_len = []
        self.content_len = []

    def process_item(self, item, spider):
        # TODO
        title_len = len(item['title'])
        author_len = len(item['author'])
        date_len = len(item['date'])
        url_len = len(item['url'])
        push_len = len(item['push'])
        content_len = len(item['content'])

        if not all([title_len, author_len, date_len, url_len, push_len, content_len]):
            raise DropItem()
        else:
            self.title_len.append(title_len)
            self.author_len.append(author_len)
            self.date_len.append(date_len)
            self.url_len.append(url_len)
            self.push_len.append(push_len)
            self.content_len.append(content_len)
            return item

    def close_spider(self, spider):
        f = lambda ls: float(sum(i > 0 for i in ls))
        name = spider.tag
        num = len(self.title_len)
        d = {}
        d['name'] = name
        d['item_num'] = '{:d}'.format(num)
        d['title_rate'] = '{:.2f}%'.format( 100 * f(self.title_len) / num)
        d['url_rate'] = '{:.2f}%'.format( 100 * f(self.url_len) / num)
        d['author_rate'] = '{:.2f}%'.format( 100 * f(self.author_len) / num)
        d['date_rate'] = '{:.2f}%'.format( 100 * f(self.date_len) / num)
        d['push_rate'] = '{:.2f}%'.format( 100 * f(self.push_len) / num)
        d['content_rate'] = '{:.2f}%'.format( 100 * f(self.content_len) / num)
        # d['title_len'] = ', '.join([str(a) for a in self.title_len])
        # d['url_len'] = ', '.join([str(a) for a in self.url_len])
        # d['author_len'] = ', '.join([str(a) for a in self.author_len])
        # d['date_len'] = ', '.join([str(a) for a in self.date_len])
        # d['push_len'] = ', '.join([str(a) for a in self.push_len])
        
        formatted_json = json.dumps(d, indent = 4)
        colorful_json = highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())
        print(colorful_json)
        spider.results = d



