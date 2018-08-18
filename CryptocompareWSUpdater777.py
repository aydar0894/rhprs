
import cryptocompare
import requests
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import time
from pymongo import MongoClient
import datetime
import dns
import sys
import os
import logging
import socketIO_client
from requests.exceptions import ConnectionError
from socketIO_client.transports import get_response
from socketIO_client.parsers import get_byte, _read_packet_text, parse_packet_text

list_coins = ["BTC", "ETH", "LTC", "ETC", "XRP", "XMR"]
list_exchanges = ["Kraken", "Poloniex", "Bitfinex", "Huobi", "Bitstamp", "LocalBitcoins", "Cryptsy", "BitBay", "BitTrex", "Exmo"]


# In[3]:



# extra function to support XHR1 style protocol
def _new_read_packet_length(content, content_index):
    packet_length_string = ''
    while get_byte(content, content_index) != ord(':'):
        byte = get_byte(content, content_index)
        packet_length_string += chr(byte)
        content_index += 1
    content_index += 1
    return content_index, int(packet_length_string)

def new_decode_engineIO_content(content):
    content_index = 0
    content_length = len(content)
    while content_index < content_length:
        try:
            content_index, packet_length = _new_read_packet_length(
                content, content_index)
        except IndexError:
            break
        content_index, packet_text = _read_packet_text(
            content, content_index, packet_length)
        engineIO_packet_type, engineIO_packet_data = parse_packet_text(
            packet_text)
        yield engineIO_packet_type, engineIO_packet_data

def new_recv_packet(self):
    params = dict(self._params)
    params['t'] = self._get_timestamp()
    response = get_response(
        self.http_session.get,
        self._http_url,
        params=params,
        **self._kw_get)
    for engineIO_packet in new_decode_engineIO_content(response.content):
        engineIO_packet_type, engineIO_packet_data = engineIO_packet
        yield engineIO_packet_type, engineIO_packet_data

setattr(socketIO_client.transports.XHR_PollingTransport, 'recv_packet', new_recv_packet)


# In[4]:

def get_timestamps(exchange):
    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    minute_ts = minute_data.find_one({"name": exchange})
    try:
        minute_ts = minute_ts["last_update"]
    except:
        next
    hourly_ts = hourly_data.find_one({"name": exchange})
    try:
        hourly_ts = hourly_ts["last_update"]
    except:
        next
    daily_ts = daily_data.find_one({"name": exchange})
    try:
        daily_ts = daily_ts["last_update"]
    except:
        next
    return [minute_ts, hourly_ts, daily_ts]




def daily_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&allData=true'    .format(symbol.upper(), comparison_symbol.upper())
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[7]:

def hourly_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym={}&limit={}&aggregate={}&toTs={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate, toTs)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[8]:

def minute_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}&toTs={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate, toTs)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[36]:

def update_collections(lim, dtype, n_timestamp, exchange, cn):
    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    hourly_data = db.hourly_data
    daily_data = db.daily_data

    if(dtype == 1):
        coin = cn
        exchange = exchange
        currency = "USD"
        try:
            res=minute_price_historical(coin, currency, limit=lim ,aggregate=1, exchange=exchange)
            if res[0]['close'].iloc[0] == 0:
                return
        except:
            return

        cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
        data = res[1]
        minute_data.update({'name': exchange, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
        minute_data.update({'name': exchange, 'Ccy': coin}, {'$set':  {'last_update': n_timestamp}}, upsert=True)

    if(dtype == 2):
        coin = cn
        exchange = exchange
        currency = "USD"
        try:
            res=hourly_price_historical(coin, currency, limit=lim ,aggregate=1, exchange=exchange)
            if res[0]['close'].iloc[0] == 0:
                return
        except:
            return

        cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
        data = res[1]
        hourly_data.update({'name': exchange, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
        hourly_data.update({'name': exchange, 'Ccy': coin}, {'$set':  {'last_update': n_timestamp}}, upsert=True)

    if(dtype == 3):
        coin = cn
        exchange = exchange
        currency = "USD"
        try:
            res=daily_price_historical(coin, currency, limit=lim ,aggregate=1, exchange=exchange)
            if res[0]['close'].iloc[0] == 0:
                return
        except:
            return

        cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
        data = res[1]
        daily_data.update({'name': exchange, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
        daily_data.update({'name': exchange, 'Ccy': coin}, {'$set':  {'last_update': n_timestamp}}, upsert=True)


# In[43]:

def on_subadd_response(*args):

    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    current_data = db.rt_updates
    minute_data = db.minute_data
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    data = list(args)[0].split('~')
    if(len(data) == 4):
        return

    if data[0] == "2":
        exchange = data[1]
        coin = data[2]
        last_updates = get_timestamps(exchange)
        print(data)
        timestamp = float(data[6])
        if(timestamp >= last_updates[0] + 60):
            limit = (timestamp - last_updates[0])/60
            update_collections(lim = limit, dtype = 1, n_timestamp = timestamp, exchange = exchange, cn = coin)
        if(timestamp >= last_updates[1] + 60*60):
            limit = (timestamp - last_updates[1])/60*60
            update_collections(lim = limit, dtype = 2, n_timestamp = timestamp, exchange = exchange, cn = coin)
        if(timestamp >= last_updates[2] + 60*60*24):
            limit = (timestamp - last_updates[2])/60*60*24
            update_collections(lim = limit, dtype = 3, n_timestamp = timestamp, exchange = exchange, cn = coin)

        price = data[5]
        current_data.update({'name': exchange, 'Ccy': coin}, {'$push': {'history' : {'timestamp' : timestamp, 'price' : price}}}, upsert=True)

def on_connect():
    print("Opened")
def on_error():
    print("error")
def on_disconnect():
    print("Closed")


# In[44]:

def run_update():
    socketIO = socketIO_client.SocketIO('https://streamer.cryptocompare.com')
    socketIO.on('connect', on_connect)
    socketIO.on('disconnect', on_disconnect)
    socketIO.on('m', on_subadd_response)
    subs = []
    for coin in list_coins:
        for exchange in list_exchanges:
            current = '2~' + exchange + '~' + coin + '~' + 'USD'
            subs.append(current)
    socketIO.emit('SubAdd',{ 'subs': subs})
    socketIO.wait()

run_update()


# In[32]:
def drop_connection():
    socketIO = socketIO_client.SocketIO('https://streamer.cryptocompare.com')
    socketIO.emit('SubRemove')
    socketIO.wait(1)


# In[ ]:




# In[ ]:
