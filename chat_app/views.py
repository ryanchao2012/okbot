from django.shortcuts import render
from django.http import HttpResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
import json
import requests
import os
import time
import random 
import jieba.posseg as pseg
import psycopg2
import logging


logger = logging.getLogger('okbot_chat_view')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setLevel(logging.INFO)
ch.setFormatter(chformatter)
logger.addHandler(ch)


OKBOT_DB_USER = os.environ['OKBOT_DB_USER']
OKBOT_DB_NAME = os.environ['OKBOT_DB_NAME']
OKBOT_DB_PASSWORD = os.environ['OKBOT_DB_PASSWORD']
CONNECT = psycopg2.connect(database=OKBOT_DB_NAME, user=OKBOT_DB_USER, password=OKBOT_DB_PASSWORD)
CURSOR = CONNECT.cursor()


OKBOT_PAGE_ACCESS_KEY=os.environ['OKBOT_PAGE_ACCESS_KEY']
OKBOT_VERIFY_TOKEN=os.environ['OKBOT_VERIFY_TOKEN']

# Create your views here.

GRAPH_API_URL = 'https://graph.facebook.com/v2.6/me/messages'

def graph_api_post(f):
    params = {
        "access_token": OKBOT_PAGE_ACCESS_KEY
    }
    headers = {
        "Content-Type": "application/json"
    }

    def graph_api_post_(*args, **kwargs):
        data, log_msg = f(*args, **kwargs)

        r = requests.post(GRAPH_API_URL, params=params, headers=headers, data=data)
        if r.status_code != 200:
            logger.warning('gragh api post failed. code: {}, message: {}'.format(r.status_code, r.text))
        else:
            logger.info('gragh api post success. {}'.format(log_msg))

    return graph_api_post_

        
    


@csrf_exempt
def fb_webhook(request):
    if request.method == 'GET':
        if request.GET.get("hub.mode") == "subscribe" and request.GET.get("hub.challenge"):
            if not request.GET.get("hub.verify_token") == OKBOT_VERIFY_TOKEN:
                return HttpResponseForbidden("Verification token mismatch")
            return HttpResponse(request.GET.get("hub.challenge"))
    elif request.method == 'POST':
        try:
            incoming = json.loads(request.body.decode('utf-8'))
        except Exception as e:
            logger.warning(e)
            return HttpResponse()

        for entry in incoming['entry']:
            for message_evt in entry['messaging']:
                sender_id = message_evt.get('sender').get('id')
                if 'message' in message_evt:
                    send_typing_bubble(sender_id, True)
                    msg = message_evt.get('message')
                    if 'text' in msg:
                        text = msg.get('text')
                        # time.sleep(random.randint(1,5))
                        handle_message(sender_id, text)
                        # print('*********', _chat_query(text))
    send_typing_bubble(sender_id, False)
    return HttpResponse()





@graph_api_post
def handle_message(sender_id, text='哈哈'):
    query = text
    reply = _chat_query(query)

    data = json.dumps({
        "recipient": {
            "id": sender_id
        },
        "message": {
            "text": reply
        },
    })
    return data, 'reply message: query: {}, reply: {}'.format(query, reply)

    
@graph_api_post
def send_typing_bubble(sender_id, onoff=False):
    if onoff:
        typing = 'typing_on'
    else:
        typing = 'typing_off'
    data = json.dumps({
        "sender_action": typing,
        "recipient": {
            "id": sender_id
        }
    })
    return data, 'send typing bubble: {}.'.format(typing)


def _chat_query(text):
    try:
        pairs = {e for e in list(pseg.cut(text)) if len(e.word.strip()) > 0}
        wlist1 = list({v.word for v in pairs})
        vocab_name = list({'--+--'.join([v.word, v.flag, 'jieba']) for v in pairs})

        CURSOR.execute("SELECT id FROM ingest_app_vocabulary WHERE name IN %s;", (tuple(vocab_name),))
        vocab_id = [v[0] for v in CURSOR.fetchall()]

        CURSOR.execute("SELECT post_id FROM ingest_app_vocabulary_post WHERE vocabulary_id IN %s;", (tuple(vocab_id),))
        post_id = [p[0] for p in CURSOR.fetchall()]

        CURSOR.execute("SELECT push, tokenized FROM ingest_app_post WHERE id IN %s;", (tuple(post_id),))
        post = [p for p in CURSOR.fetchall()]

        pscore = [None] * len(post)
        for i in range(len(post)):
            wlist2 = post[i][1].split()
            pscore[i] = _jaccard(wlist1, wlist2)

        top_post = post[pscore.index(max(pscore))]
        push = [p[p.find(':')+1 :].strip() for p in top_post[0].split('\n')]
        select_push = push[random.randint(0, len(push)-1)]
        return select_push

    except Exception as e:
        CONNECT.rollback()
        logger.error(e)
        return 'Q_Q'



def _jaccard(wlist1, wlist2):
    wset1 = set(wlist1)
    wset2 = set(wlist2)
    return len(wset1.intersection(wset2)) / len(set(wlist1 + wlist2))



