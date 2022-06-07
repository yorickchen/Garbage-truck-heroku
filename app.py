import os
from datetime import datetime, timedelta

from flask import Flask, abort, request

# https://github.com/line/line-bot-sdk-python
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, LocationMessage, LocationSendMessage

import enum
import math
import requests
import csv
import redis
import json
from urllib.parse import urlencode

app = Flask(__name__)

line_bot_api = LineBotApi(os.environ.get("CHANNEL_ACCESS_TOKEN"))
handler = WebhookHandler(os.environ.get("CHANNEL_SECRET"))
realtime_data_url = os.environ.get("REALTIME_DATA_URL")
covid19_screen_data_url = os.environ.get("COVID19_SCREEN_DATA_URL")
weather_tw_url = os.environ.get("WEATHER_TW_URL")
weather_tw_token = os.environ.get("WEATHER_TW_TOKEN")
epa_gov_token = os.environ.get("EPA_GOV_TOKEN")
redis_host = os.environ.get("REDIS_HOST")
redis_port = os.environ.get("REDIS_PORT")
redis_pwd = os.environ.get("REDIS_PWD")
epa_gov_url = 'https://data.epa.gov.tw/api/v2/FAC_P_07'
home_city = '三重區'
home_lat = 25.078088032882395
home_lng = 121.49169181080875
range_distance = 250
city_list = [
    '高雄市','雲林縣','金門縣','連江縣','苗栗縣','花蓮縣','臺東縣','臺南市','臺北市','臺中市',
    '澎湖縣','桃園市','新竹縣','新竹市','新北市','彰化縣','屏東縣','宜蘭縣','基隆市','嘉義縣',
    '嘉義市','南投縣'
]
toilet_distance = 300

class WeatherMethod(enum.Enum):
    TwoDay = 'F-D0047-069'

class Route(enum.IntEnum):
    Realtime = 1
    Weather = 2
    Covid19Screen = 3
    UpdateToilet = 4
    Toilet = 5

class LabRedis(): 
    def __init__(self, host: str, port: int, pwd: str):
        self.redis = redis.StrictRedis(host=host, port=port, password=pwd, charset='utf-8')
    
    def set_json(self, key, value):
        self.redis.set(key, json.dumps(value))
    
    def get_json(self, key):
        value = self.redis.get(key)
        if value:
            return json.loads(value)
        return None

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
    reply_msg = None
    if route == Route.Realtime:
        reply_msg = get_realtime()
    elif route == Route.Weather:
        reply_msg = get_weather()
    elif route == Route.Covid19Screen:
        reply_msg = get_covid19_screening()
    elif route == Route.UpdateToilet:
        reply_msg = update_toilet()
    if reply_msg:
        reply = TextSendMessage(text=f"{reply_msg}")
        line_bot_api.reply_message(event.reply_token, reply)

@handler.add(MessageEvent, message=LocationMessage)
def handle_location_message(event):
    reply_msgs = get_toilets(event.message.latitude, event.message.longitude, event.message.address)
    if len(reply_msgs) > 0:
        line_bot_api.reply_message(event.reply_token, reply_msgs)

def route_message(msg) -> Route:
    if msg.lower() in ('go', '垃圾'):
        return Route.Realtime
    elif msg.lower() in ('weather', '天氣', '氣象'):
        return Route.Weather
    elif msg.lower() in ('篩檢', '篩檢量', '檢測', '檢測量'):
        return Route.Covid19Screen
    elif msg.lower() in ('更新廁所資料'):
        return Route.UpdateToilet
    return None

def get_realtime():
    rows = requests.get(realtime_data_url)
    zones = []
    for row in rows.json():
        if row.get('cityName') == home_city:
            distance = get_home_distance(row.get('longitude'), row.get('latitude'))
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

def get_distance(lat1, lng1, lat2, lng2):
    EARTH_REDIUS = 6378.137
    def rad(d):
        return d * math.pi / 180.0
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)
    s = 2 * math.asin(math.sqrt(math.pow(math.sin(a/2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b/2), 2)))
    s = s * EARTH_REDIUS
    return s * 1000 # 公尺

def get_home_distance(longitude, latitude):
    return get_distance(float(latitude), float(longitude), home_lat, home_lng)

def get_covid19_screening():
    msg = ''
    try:
        today = datetime.today().date()
        resp = requests.get(covid19_screen_data_url)
        rows = csv.reader(resp.text.splitlines())
        for i, row in enumerate(rows):
            if i > 2:
                dt = datetime.strptime(row[0],'%Y/%m/%d').date()
                if dt < today and (today - dt).days < 14:
                    msg += f'{row[0]}({weekDayText(dt.weekday())}): {int(float(row[-1]))}\n'
    except:
        msg = 'error'
    return msg

def get_weather():
    qstr = urlencode({
        'Authorization': weather_tw_token, 
        'format': 'JSON', 
        'locationName': '三重區',
        'elementName': 'PoP6h,AT',
        'sort': 'startTime'
    })
    qurl = '{}/{}?{}'.format(weather_tw_url, WeatherMethod.TwoDay.value, qstr)
    result = requests.get(qurl).json()
    locations = result.get('records').get('locations')
    if len(locations) > 0:
        location = locations[0].get('location')
        if len(location) > 0:
            msgs = []
            records = location[0].get('weatherElement')
            for record in records:
                if record.get('elementName') == 'PoP6h':
                    # 降雨機率
                    msgs.append(parsePoP6HData(record.get('time')))
                elif record.get('elementName') == 'AT':
                    # 體感溫度
                    msg = parseATData(record.get('time'))
                    if msg:
                        msgs.append(msg)
            return '\n'.join(msgs)
    return None          

def get_toilets(latitude, longitude, address):
    match_city = None
    if address:
        # 有地址的話, 嘗試取出縣市名稱
        for city in city_list:
            if city in address:
                match_city = city
                break
    db = LabRedis(host=redis_host, port=int(redis_port), pwd=redis_pwd)
    if match_city:
        check_cities = [match_city]
    else:
        check_cities = city_list
    for check_city in check_cities:
        zone_toilets = [] # 範圍內的廁所
        toilets = db.get_json(check_city)
        for toilet in toilets:
            distance = get_distance(float(toilet['lat']), float(toilet['lng']), latitude, longitude)
            if distance < toilet_distance:
                toilet['distance'] = distance
                zone_toilets.append(toilet)
        if len(zone_toilets) > 0:
            msgs = []
            # 排序後取前5筆
            zone_toilets.sort(key=lambda k: k['distance'])
            for zone_toilet in zone_toilets[:5]:
                msgs.append(LocationSendMessage(
                    title=f"{zone_toilet['name']}({zone_toilet['grade']})",
                    address=zone_toilet['address'],
                    latitude=float(zone_toilet['lat']),
                    longitude=float(zone_toilet['lng'])
                ))
            msgs.append(TextSendMessage(text=f'找到附近{toilet_distance}公尺內的公廁'))           
            return msgs
    return [TextSendMessage(text=f'附近{toilet_distance}公尺內找無公廁')]

def update_toilet():
    offset = 0
    limit = 200
    country_data = dict()
    while offset >= 0:
        url = f"{epa_gov_url}?format=json&offset={offset}&limit={limit}&api_key={epa_gov_token}"
        resp = requests.get(url)
        if len(resp.json()['records']) > 0:
            offset += limit
            for record in resp.json()['records']:
                country = record['country']
                if country not in country_data:
                    country_data[country] = []
                country_data[country].append({
                    'name': record['name'],
                    'address': record['address'],
                    'lat': record['latitude'],
                    'lng': record['longitude'],
                    'grade': record['grade'],
                    'type': record['type'],
                    'type2': record['type2']
                })
        else:
            offset = -1
    db = LabRedis(host=redis_host, port=int(redis_port), pwd=redis_pwd)
    for _country, data in country_data.items():
        db.set_json(_country, data)
    return 'done'

def getWeatherEmoji(pop):
    if pop <= 10:
        return chr(int('0x1000A9', 16))
    elif pop <= 40 and pop > 10:
        return chr(int('0x1000AC', 16))
    elif pop <= 70 and pop > 40:
        return chr(int('0x10003A', 16))
    else:
        return chr(int('0x1000AA', 16))

def parsePoP6HData(times):
    msgs = []
    for t in times:
        time_text = ''
        start = datetime.strptime(t.get('startTime'),'%Y-%m-%d %H:%M:%S')
        if start.hour == 0:
            continue # ignore 清晨
        elif start.hour == 6:
            time_text = '上午'
        elif start.hour == 12:
            time_text = '下午'
        elif start.hour == 18:
            time_text = '晚上'
        pop = int(t.get("elementValue")[0].get('value'))
        msgs.append(f'{start.month}/{start.day}({weekDayText(start.weekday())}){time_text}:{pop}%{getWeatherEmoji(pop)}')
    return '降雨機率\n' + '\n'.join(msgs)

def parseATData(times):
    dts = {}
    for t in times:
        dt = datetime.strptime(t.get('dataTime'),'%Y-%m-%d %H:%M:%S')
        dt_key = f'{dt.month}/{dt.day}({weekDayText(dt.weekday())})'
        if dt_key not in dts:
            dts[dt_key] = []
        dts[dt_key].append(t)
    at_text = '體感溫度\n'
    for k, data in dts.items():
        max_at = 0
        min_at = 100
        for d in data:
            at = int(d.get('elementValue')[0].get('value'))
            if at > max_at:
                max_at = at
            if at < min_at:
                min_at = at
        if max_at > 0 and min_at < 100:
            at_text += f'{k} {min_at}℃ ~ {max_at}℃\n'
    return at_text

def weekDayText(weekday):
    if weekday == 0:
        return '一'
    elif weekday == 1:
        return '二'
    elif weekday == 2:
        return '三'
    elif weekday == 3:
        return '四'
    elif weekday == 4:
        return '五'
    elif weekday == 5:
        return '六'
    elif weekday == 6:
        return '日'