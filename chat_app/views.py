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
                    send_typing_bubble(sender_id)
                    time.sleep(random.randint(1,5))
                    handle_message(sender_id, message_evt.get('message'))
        
    return HttpResponse()



def handle_message(sender_id, msg):
    if 'text' in msg:
        params = {
            "access_token": OKBOT_PAGE_ACCESS_KEY
        }
        headers = {
            "Content-Type": "application/json"
        }

        reply = msg.get('text')
        data = json.dumps({
            "recipient": {
                "id": sender_id
            },
            "message": {
                "text": reply
            },
        })
        r = requests.post("https://graph.facebook.com/v2.6/me/messages", params=params, headers=headers, data=data)
        if r.status_code != 200:
            logging.warning('post request to gragh api failed. code: {}, message: {}'.format(r.status_code, r.text))
        else:
            logging.info('post request to gragh api success. query: {}, reply: {}'.format(msg.get('text'), reply))


def send_typing_bubble(sender_id):
    params = {
        "access_token": OKBOT_PAGE_ACCESS_KEY
    }
    headers = {
        "Content-Type": "application/json"
    }
    data = json.dumps({
        "sender_action": "typing_on",
        "recipient": {
            "id": sender_id
        }
    })
    r = requests.post("https://graph.facebook.com/v2.6/me/messages", params=params, headers=headers, data=data)
    if r.status_code != 200:
        logging.warning('post request to gragh api failed. code: {}, message: {}'.format(r.status_code, r.text))
    else:
        logging.info('post request to gragh api success. send typing indicator.')
    
