from django.shortcuts import render
from django.http import HttpResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
import logging
import json
import requests
import os
import time
import random 

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')

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
            logging.warning('gragh api post failed. code: {}, message: {}'.format(r.status_code, r.text))
        else:
            logging.info('gragh api post success. {}'.format(log_msg))

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
            logging.warning(e)
            return HttpResponse()

        for entry in incoming['entry']:
            for message_evt in entry['messaging']:
                sender_id = message_evt.get('sender').get('id')
                if 'message' in message_evt:
                    logging.info(repr(message_evt.get('message')))
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
    
