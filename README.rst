MarginalBear
============

**MarginalBear** is a chit-chatbot with a conversation retrieval engine based on PTT corpus.
The core modules in this repo are: ``crawl_app``, ``ingest_app`` and ``chat_app``, and we use ``Django`` to manage these apps.


<img src="res/icon.png" width="200">
<img src="res/qrcode.png" width="160">


PTT-Crawler
-----------
Crawlers are implemented with ``scrapy`` framework, the logic is defined under ``crawl_app/spider/`` directory, each article in crawled data is collected in jsonline files and formatted as follows:

	"url": <url>,
	"data": <article-publish-date>,
	"title": <title>,
	"author": <author>,
	"content": <article-body>,
	"push": <list of comment-string>,


To build conversation corpus, we paired the ``title`` and ``push`` fields to mimic the Q&A behavior, here are some examples:

	<title> as Q              <push> as A
	綜藝玩很大是不是走下坡了      很久沒看了  都是老梗
	該怎麼挽回好友？             就算挽回 以後也會因為別的事離開你
	妹妹想去補習，該怎麼辦        其實你沒有妹妹
	

Further data cleaning process is handled by ``ingest_app``.

Each crawler only handles articles from one PTT forum, since the user habits in different forums(ex: gossiping, sex, mantalk, ... etc.) are usually quit different, we may apply specific rules on each crawler. 
In order to manage these crawlers easily, the crawl engine are integrated with Django. In Django admin interface, we can easily create different rules to filter out the noisy articles. A rule is actually a blacklist set with ``phrases`` should be filtered and a ``type`` related to the field of crawled items, these types are:

- ``title``: related to ``title`` field of crawled items.
- ``push``: related to ``push`` field of crawled items.
- ``author``: related to ``author `` field of crawled items.
- ``audience``: related to commenter of ``push`` field.

A blacklist can be defined in admin as:

	"type": title,
	"phrase": 公告, Re:, Fw:, 投稿, 水桶,

Which means crawler should drop the item as the article's title contains one of these phrases. With this configuration, each crawler can equip multiple rules to aim different kind of censored contents.


A spider can be defined in admin as:

	"tag": Gossiping,  # forum name
	"entry": https://www.ptt.cc/bbs/Gossiping/index{index}.html,
	"page": 250,   # pages to crawl in a crawl task
	"offset": 50,  # the distance from the newest page
	"freq": 1,     # crawl frequencey, used with crontab, ex: daily
	"blacklist": [<rule1>, <rule2>, ...],
	"start": -1,   # start page index
	"end": -1,     # end page index
	"status": debug, # pass or debug

When a spider is created, run this command to check whether the config is valid:

    ./manage.py okbot_update_spider <tag>

The ``start`` and ``end`` index will be updated according to ``page`` and ``offset`` settings, if everything goes fine, the ``status`` will change to ``pass``, meaning the spider is ready to fire:

    ./manage.py okbot_crawl <tag>

After issuing a crawl task, a job log is generated; when the task is finished, a crawl summary is recorded and can be viewed in admin, ex:

	"name": "Gossiping",
	"item_num": "3227",
	"drop_num": "10",
	"title": "mean: 19.2, std: 4.3",
	"url": "mean: 56.0, std: 0.0",
	"author": "mean: 16.7, std: 4.2",
	"date": "mean: 24.0, std: 0.0",
	"push": "mean: 17.4, std: 9.4",
	"content": "mean: 269.3, std: 350.1"

Finally, we use crontab to manage daily crawl jobs, you can find the handler script in ``crawl_ingest.py``.


Ingester
--------
This module "ingest" crawled data into database, and does three things:

1. Build vocabularies by tokenizing(with ``jieba``) articles' titles.
2. Index every articles.
3. Build the ``ManyToMany`` relation(inverted indexing) between vocaluaries and articles.   

The taskes are wrapped into a command:

    ./manage.py okbot_ingest --jlpath <jsonline-file> --tokenizer <tokenizer>

Since the script only support ``postgresql``， if you use postgresql backend with Django, provide these environment variables, then the command should work:

- `OKBOT_DB_USER`
- `OKBOT_DB_NAME`
- `OKBOT_DB_PASSWORD`
 
The vocabulary will be listed in Django admin. 
Since retrieval mechanism works with inverted index, you should label the words with high document-frequecy as ``stopword`` or the retrieval process will be very slow.  



Chatbot
-------

The bot is deployed on both messenger and line platforms, you can find the api implementation in ``chat_app/views.py``. Basically, when the bot recieves a query, the engine find the related articles by inverted index, then calculates the ``jaccard`` or ``bm25`` similarity with some other features between query and articles' titles, after ranking the articles, the bot finally picks an "comment" in the top ranking articles as an reponse. You can find the ranking algorithm and implementations in ``chat_app/bots.py``.
A ``word2vec``(with ``gensim`` package) model is also applied on queries to generate similar phrases, in order to rich the search informations.

Other features:

- Chat rules table
- Chat tree/caching
- Jieba tag weighting table
 


Evaluation
----------

