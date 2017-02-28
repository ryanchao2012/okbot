
def okbot_query_psql(f):
    import psycopg2
    def okbot_query_postgres_(*args, **kwargs):
        # TODO:
        # cnn = psycopg2.connect(...)
        # cur = cnn.cursor()
        # try:
        #     rv = f(cnn, *args, **kwargs)
        # except Exception as e:
        #     cnn.rollback()
        #     raise
        # finally:
        #     cnn.close()
        # return rv
        pass
    
    return okbot_query_postgres_




@okbot_query_psql
def list_spiders():
    # TODO:
    # if not os.path.isfile(CONF_FILE):
    #   print('Spider config file is not found')
    #   sys.exit()

    # df = pd.read_csv(CONF_FILE)
    # print('\n\033[93mAll {} spiders:'.format(len(df.name)), end = '')
    # spiders = ', '.join(list(df.name.values))
    # print('\033[33m')
    # print(spiders)
    # return spiders
    pass


@okbot_query_psql
def get_spider(spider_name=None):
    # TODO:
    # if not spider_name:
    #     return None
    # else:
    #     pass
    # return spider
    pass

def test_spider(spider_name=''):
    from scrapy.settings import Settings
    from scrapy.crawler import CrawlerProcess
    from single import SingleSpider
    import time


    # TODO:
    # if os.path.isfile('output.jl'): os.remove('output.jl')
    # if os.path.isfile('log.txt'): os.remove('log.txt')

    # df = pd.read_csv('spider-config.csv')
    # if not spider_name in df.name.values:
    #     print('spider name: ' + spider_name + ' is not found')
    #     sys.exit()
    # idx = df[df.name == spider_name].index.tolist()[0]

    # config = df.iloc[idx]

    # settings = Settings()
    # settings.set('FEED_URI', 'output.jl')
    # settings.set('LOG_FILE', 'log.txt')
    # settings.set('FEED_FORMAT', 'jsonlines')
    # settings.set('LOG_LEVEL', 'WARNING')
    # settings.set('USER_AGENT', 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)')
    # settings.set('ITEM_PIPELINES', {'pipelines.PttSpiderPipeline' : 400})
    
    # t = time()
    # process = CrawlerProcess(settings)
    # process.crawl(SingleSpider, tag= config['name'],
    #     entry=config.entry_url, num_page=config.num_page, 
    #     url_xpath=config.url_xpath, title_xpath=config.title_xpath, 
    #     author_xpath=config.author_xpath, date_xpath=config.date_xpath,
    #     content_xpath=config.content_xpath, 
    #     author_pattern=config.author_pattern, date_pattern=config.date_pattern)

    # process.start()

    print('good')
    print(time.time() - t)