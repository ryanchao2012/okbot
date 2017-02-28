from django.shortcuts import render
from django.http import HttpResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
import json
import requests
import os
import time
import random 

import logging
logger = logging.getLogger('okbot_crawl')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setLevel(logging.INFO)
ch.setFormatter(chformatter)
logger.addHandler(ch)


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
                    time.sleep(random.randint(1,5))
                    send_typing_bubble(sender_id, False)
                    handle_message(sender_id, message_evt.get('message'))
        
    return HttpResponse()


@graph_api_post
def handle_message(sender_id, msg):
    query, reply = '', ''
    if 'text' in msg:
        query = msg.get('text')
        reply = query

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
    
