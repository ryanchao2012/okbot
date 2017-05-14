import json
import logging
import os
import re

from django.http import HttpResponse, HttpResponseForbidden
from django.http.response import HttpResponseBadRequest
from django.template.response import TemplateResponse
from django.views.decorators.csrf import csrf_exempt


# from django.shortcuts import render
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    FollowEvent, ImageSendMessage, JoinEvent, LeaveEvent,
    MessageEvent, SourceGroup, SourceRoom, SourceUser,
    TextMessage, TextSendMessage, UnfollowEvent,
)

import requests

from .bots import LineBot, MessengerBot


logger = logging.getLogger('okbot_chat_view')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setLevel(logging.INFO)
ch.setFormatter(chformatter)
logger.addHandler(ch)


line_bot_api = LineBotApi(os.environ['LINE_CHANNEL_ACCESS_TOKEN'])
line_webhook_parser = WebhookParser(os.environ['LINE_CHANNEL_SECRET'])
DISCLAIMER = (
    "免責聲明：\n"
    "三杯熊的所有回應皆以ptt上的內容為素材，若您因為惡意連結或爭議性內容受到任何損失，\n"
    "秉持著都是they的錯的精神，三杯熊將不負賠償責任。\n"
    "若您有任何疑慮，請直接像三杯熊反應，但是我聽到後就不必再大聲了。\n"
    "若您還是想與三杯熊聊天，請對著手機大聲汪汪汪叫三聲，即可開始用文字訊息聊天。")


OKBOT_PAGE_ACCESS_KEY = os.environ['OKBOT_PAGE_ACCESS_KEY']
OKBOT_VERIFY_TOKEN = os.environ['OKBOT_VERIFY_TOKEN']

# Create your views here.

GRAPH_API_URL = 'https://graph.facebook.com/v2.6/me/messages'


def home(request):
    response = TemplateResponse(request, 'privacypolicy.html', {})
    return response


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
                        utype, uid = _user_id(event.source)
                        if query == '三杯熊滾' and utype != 'user':
                            reply = '你會後悔'
                            line_bot_api.reply_message(
                                event.reply_token,
                                TextSendMessage(text=reply))
                            _leave(utype, uid)
                            logger.info('leaving: utype: {}, uid: {}, query: {}'.format(utype, uid, query))
                        else:
                            reply = LineBot(query, 'line', uid, utype).retrieve()
                            line_bot_api.reply_message(
                                event.reply_token,
                                _message_obj(reply))
                            logger.info('reply message: utype: {}, uid: {}, query: {}, reply: {}'.format(utype, uid, query, reply))
                    except Exception as err:
                        logger.error('okbot.chat_app.line_webhook, message: {}'.format(err))
            elif isinstance(event, FollowEvent) or isinstance(event, JoinEvent):
                try:
                    query = '<FollowEvent or JoinEvent>'
                    reply = DISCLAIMER
                    utype, uid = _user_id(event.source)
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=reply))
                    logger.info('reply message: utype: {}, uid: {}, query: {}, reply: {}'.format(utype, uid, query, reply))
                except Exception as err:
                    logger.error('okbot.chat_app.line_webhook, message: {}'.format(err))
            elif isinstance(event, UnfollowEvent) or isinstance(event, LeaveEvent):
                try:
                    query = '<UnfollowEvent or LeaveEvent>'
                    utype, uid = _user_id(event.source)
                    logger.info('leave or unfollow: utype: {}, uid: {}, query: {}'.format(utype, uid, query))
                except Exception as err:
                    logger.error('okbot.chat_app.line_webhook, message: {}'.format(err))

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
        print('==========')
        print(incoming)
        print('==========')
        for entry in incoming['entry']:
            for message_evt in entry['messaging']:
                sender_id = message_evt.get('sender').get('id')
                if 'message' in message_evt:
                    send_seen(sender_id)
                    send_typing_bubble(sender_id, True)
                    msg = message_evt.get('message')
                    if 'text' in msg:
                        text = msg.get('text')
                        handle_messenger(sender_id, text)
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
def handle_messenger(sender_id, text='哈哈'):
    query = text
    reply = MessengerBot(query, 'messenger', '1234567').retrieve()
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


def _message_obj(reply):
    if 'imgur' in reply:
        match_web = re.search(r'http:\/\/imgur\.com\/[a-z0-9A-Z]{7}', reply)
        match_jpg = re.search(r'http:\/\/(i|m)\.imgur\.com\/[a-z0-9A-Z]{7}\.jpg', reply)
        if match_web:
            match = match_web.group()
        elif match_jpg:
            match = match_jpg.group()
        else:
            match = reply
        imgur_url = re.sub('http', 'https', match)
        return ImageSendMessage(original_content_url=imgur_url,
                                preview_image_url=imgur_url)
    else:
        return TextSendMessage(text=reply)


def _user_id(source):
    if isinstance(source, SourceUser):
        utype = 'user'
        uid = source.user_id
    elif isinstance(source, SourceGroup):
        utype = 'group'
        uid = source.group_id
    elif isinstance(source, SourceRoom):
        utype = 'room'
        uid = source.room_id
    return utype, uid


def _leave(utype, uid):
    try:
        if utype == 'group':
            line_bot_api.leave_group(uid)
        elif utype == 'room':
            line_bot_api.leave_room(uid)
    except LineBotApiError as err:
        logger.error('okbot.chat_app.leave, message: {}'.format(err))
