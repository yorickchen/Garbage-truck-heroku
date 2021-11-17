import os
from datetime import datetime

from flask import Flask, abort, request

# https://github.com/line/line-bot-sdk-python
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

import enum
import math
import requests

app = Flask(__name__)

line_bot_api = LineBotApi(os.environ.get("CHANNEL_ACCESS_TOKEN"))
handler = WebhookHandler(os.environ.get("CHANNEL_SECRET"))
realtime_data_url = os.environ.get("REALTIME_DATA_URL")
home_city = '三重區'
home_lat = 25.078088032882395
home_lng = 121.49169181080875
range_distance = 500

class Route(enum.IntEnum):
    Realtime = 1

@app.route("/", methods=["GET", "POST"])
def callback():
    if request.method == "GET":
        return "Hello Heroku"
    if request.method == "POST":
        signature = request.headers["X-Line-Signature"]
        body = request.get_data(as_text=True)
        try:
            handler.handle(body, signature)
        except InvalidSignatureError:
            abort(400)
        return "OK"

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    get_message = event.message.text

    route = route_message(get_message)
    if route == Route.Realtime:
        reply_msg = get_realtime()
        reply = TextSendMessage(text=f"{reply_msg}")
        line_bot_api.reply_message(event.reply_token, reply)

def route_message(msg) -> Route:
    if msg.lower() in ('go', 'start'):
        return Route.Realtime
    return None

def get_realtime():
    rows = requests.get(realtime_data_url)
    zones = []
    for row in rows.json():
        if row.get('cityName') == home_city:
            distance = get_distance(row.get('longitude'), row.get('latitude'))
            print(f"lng={row.get('longitude')}, lat={row.get('latitude')}, distance = {distance}")
            if distance < range_distance:
                zones.append({
                    'location': row.get('location'),
                    'distance': distance
                })
    sorted_zones = sorted(zones, key=lambda x: x['distance'])
    reply_msgs = []
    for sorted_zone in sorted_zones:
        reply_msgs.append(f"{sorted_zone['location']}({sorted_zone['distance']})")
    return '\n'.join(reply_msgs) if len(reply_msgs) > 0 else '附近沒有垃圾車'

def get_distance(longitude, latitude):
    lng_dist = (float(longitude) - home_lng) * 10000
    lat_dist = (float(latitude) - home_lat) * 10000
    distance = math.pow(lng_dist, 2) + math.pow(lat_dist, 2)
    return math.pow(distance, 0.5) * 10
