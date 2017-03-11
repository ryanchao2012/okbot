from django.core.management.base import BaseCommand, CommandError
import jieba.posseg as pseg

OKBOT_DB_USER = os.environ['OKBOT_DB_USER']
OKBOT_DB_NAME = os.environ['OKBOT_DB_NAME']
OKBOT_DB_PASSWORD = os.environ['OKBOT_DB_PASSWORD']

MAX_DOC_FREQ = 50

class Command(BaseCommand):
    help = '''
           query ptt comment by giving title string,
           '''
    def add_arguments(self, parser):
        parser.add_argument('query', nargs=1, type=str)
        parser.add_argument('--tokenizer', nargs=1, type=str)


    def handle(self, *args, **options):
        self.conn = psycopg2.connect(database=OKBOT_DB_NAME, user=OKBOT_DB_USER, password=OKBOT_DB_PASSWORD)
        self.cur = self.conn.cursor()
        query = options['query'][0]
        tokenizer = options['tokenizer'][0]

        pairs = {e for e in list(pseg.cut(query)) if len(e.word.strip()) > 0}
        wlist1 = list({v.word for v in pairs})
        vocab_name = list({'--+--'.join([v.word, v.flag, tokenizer]) for v in pairs})

        self.cur.execute("SELECT id FROM ingest_app_vocabulary WHERE name IN %s AND doc_freq < 50;", (tuple(vocab_name),))
        vocab_id = [v[0] for v in self.cur.fetchall()]

        self.cur.execute("SELECT post_id FROM ingest_app_vocabulary_post WHERE vocabulary_id IN %s;", (tuple(vocab_id),))
        post_id = [p[0] for p in self.cur.fetchall()]

        self.cur.execute("SELECT (title, push) FROM ingest_app_post WHERE id IN %s;", (tuple(post_id),))

        post = [[p for p in self.cur.fetchall()]]

        pscore = [None] * len(post)
        for i in len(post):
            pairs = {e for e in list(pseg.cut(post[i][0])) if len(e.word.strip()) > 0}
            wlist2 = list({v.word for v in pairs})
            pscore[i] = _jaccard(wlist1, wlist2)

        top_post = post[pscore.index(max(pscore))]

        print(top_post)






def _jaccard(wlist1, wlist2):
    wset1 = set(wlist1)
    wset2 = set(wlist2)
    return len(wset1.intersection(wset2)) / len(set(wlist1 + wlist2))