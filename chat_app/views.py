from django.shortcuts import render
from django.http import HttpResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

import json
import requests
import os
import time
import random 
import jieba.posseg as pseg
import logging
from utils import (PsqlQuery, Tokenizer, 
        bm25_similarity, jaccard_similarity, 
        tfidf_jaccard_similarity)


logger = logging.getLogger('okbot_chat_view')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setLevel(logging.INFO)
ch.setFormatter(chformatter)
logger.addHandler(ch)


line_bot_api = LineBotApi(os.environ['LINE_CHANNEL_ACCESS_TOKEN'])
line_webhook_parser = WebhookParser(os.environ['LINE_CHANNEL_SECRET'])


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
def line_webhook(request):
    if request.method == 'POST':
        signature = request.META['HTTP_X_LINE_SIGNATURE']
        body = request.body.decode('utf-8')

        try:
            events = line_webhook_parser.parse(body, signature)
        except InvalidSignatureError:
            return HttpResponseForbidden()
        except LineBotApiError:
            return HttpResponseBadRequest()

        for event in events:
            if isinstance(event, MessageEvent):
                if isinstance(event.message, TextMessage):
                    try:
                        query = event.message.text
                        reply = _chat_query(query)
                        logger.info('reply message: query: {}, reply: {}'.format(query, reply))
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=reply) 
                        )
                    except Exception as e:
                        logger.error('okbot.chat_app.line_webhook, message: {}'.format(e))

        return HttpResponse()
    else:
        return HttpResponseBadRequest()    


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
                    send_seen(sender_id)
                    send_typing_bubble(sender_id, True)
                    msg = message_evt.get('message')
                    if 'text' in msg:
                        text = msg.get('text')
                        handle_message(sender_id, text)
    send_typing_bubble(sender_id, False)
    return HttpResponse()


@graph_api_post
def send_seen(sender_id):
    data = json.dumps({
        "sender_action": 'mark_seen',
        "recipient": {
            "id": sender_id
        }
    })
    return data, 'send mark seen.'

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
        tok, words, flags = Tokenizer('jieba').cut(text)
        vocab_name = ['--+--'.join([t.word, t.flag, 'jieba']) for t in tok]
        psql = PsqlQuery()
        query_vocab = list(psql.query( '''
                                                SELECT * FROM ingest_app_vocabulary 
                                                WHERE name IN %s;
                                            ''', (tuple(vocab_name),)
                                    )
        )
        vschema = psql.schema

        tag_weight = {}
        for q in query_vocab:
            if q[vschema['tag']][0] == 'n':
                tag_weight[q] = 2.0
            elif q[vschema['tag']][0] == 'v':
                tag_weight[q] = 1.2
            elif q[vschema['tag']][0] == 'i':
                tag_weight[q] = 2.5
            else: tag_weight[q] = 1.0

        vocab = [{'word': ':'.join([q[vschema['word']], q[vschema['tag']]]),'termweight': tag_weight[q], 'docfreq': q[vschema['doc_freq']]} for q in query_vocab]
        

        query_vid = [q[vschema['id']] for q in query_vocab if not (q[vschema['stopword']]) and q[vschema['doc_freq']] < 10000 ]
        print(vocab)

        query_pid = list(PsqlQuery().query( '''
                                                SELECT post_id FROM ingest_app_vocabulary_post 
                                                WHERE vocabulary_id IN %s;
                                            ''', (tuple(query_vid),)
                                    )
        )
        psql = PsqlQuery()
        allpost = psql.query('''
                            SELECT tokenized, grammar, push, url FROM ingest_app_post WHERE id IN %s;
                          ''', (tuple(query_pid),)
        )
        pschema = psql.schema

        tfidf_top_post = []
        tfidf_top_score = -9999.0
        jaccard_top_post = []
        jaccard_top_score = -9999.0
        tolerance = 0
        for post in allpost:
            doc = [':'.join([t, g]) for t, g in zip(post[pschema['tokenized']].split(), post[pschema['grammar']].split())]
            score = tfidf_jaccard_similarity(vocab, doc)
            # score = bm25_similarity(vocab, doc)
            if score + tolerance >= tfidf_top_score:
                tfidf_top_score = score
                tfidf_top_post = [post]
            score = jaccard_similarity(vocab, doc)
            if score + tolerance >= jaccard_top_score:
                jaccard_top_score = score
                jaccard_top_post = [post]

        logger.info('#{:.2f}:Top post(tfidf): {}, {}'.format(tfidf_top_score, [ p[pschema['tokenized']] for p in tfidf_top_post], tfidf_top_post[0][pschema['url']]))
        logger.info('#{:.2f}:Top post(jaccard): {}'.format(jaccard_top_score, [ p[pschema['tokenized']] for p in jaccard_top_post]))
        final_post = tfidf_top_post[random.randint(0, len(tfidf_top_post)-1)]
        push = [p[p.find(':')+1 :].strip() for p in final_post[pschema['push']].split('\n')]
        select_push = push[random.randint(0, len(push)-1)]
        return select_push

    except Exception as e:
        logger.error(e)
        default_reply = ['嗄', '三小', '滾喇', '嘻嘻']

        return default_reply[random.randint(0, len(default_reply)-1)]




