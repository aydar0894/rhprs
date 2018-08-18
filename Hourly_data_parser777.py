
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
from socketIO_client.transports import get_response
from socketIO_client.parsers import get_byte, _read_packet_text, parse_packet_text


list_coins = ["BTC", "ETH", "LTC", "ETC", "XRP", "XMR"]
list_exchanges = ["Kraken", "Poloniex", "Bitfinex", "Huobi", "Bitstamp", "LocalBitcoins", "Cryptsy", "BitBay", "BitTrex", "Exmo"]

# In[2]:

from requests.exceptions import ConnectionError

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


# In[3]:

def daily_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&allData=true'    .format(symbol.upper(), comparison_symbol.upper())
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[4]:

def hourly_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym={}&limit={}&aggregate={}&toTs={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate, toTs)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[5]:

def minute_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}&toTs={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate, toTs)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[11]:

def parse():
    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    statistics_mhd = db.statistics_mhd
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    cntr_d = 0
    #Daily data

    for exc in list_exchanges:
        for coin in list_coins:
            cntr_d += 1
            coin            = coin
            exchange        = exc
            countercurrency = "USD"
            res = daily_price_historical(coin, countercurrency, exchange)
            cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
            data = res[1]
            daily_data.update({'name': exchange, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
            print("Iteration Daily " + str(cntr_d) + ":\n")
            daily_data.update({'name': exchange, 'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)
        time.sleep(3)

    #Hourly data
    for exc in list_exchanges:
        for coin in list_coins:
            date_tmp = int(time.time())
            cntr = 0
            coin            = coin
            exchange        = exc
            countercurrency = "USD"
            date_from = int(time.mktime(datetime.datetime.strptime("2009-01-09-00:00:00","%Y-%m-%d-%H:%M:%S").timetuple()))
            flag_coin_skip = False
            while date_tmp > date_from:
                cntr += 1
                try:
                    res=hourly_price_historical(coin, countercurrency, toTs=date_tmp, limit=(30*24) ,aggregate=1, exchange=exchange)
                    if res[0]['close'].iloc[0] == 0:
                        break
                except:
                    # if no data found at all
                    if cntr == 1:
                        print("      No data for currency available, Skipping")
                    flag_coin_skip = True

                if flag_coin_skip == True:
                    break

                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                df = res[0]
                df = df[cols]
                date_tmp=df['time'].iloc[0]
                data = res[1]
                hourly_data.update({'name': exchange, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
                print("Iteration Hourly " + str(cntr) + ":\n")
                time.sleep(3)
            hourly_data.update({'name': exchange, 'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)

    #Minute data
    for exc in list_exchanges:
        for coin in list_coins:
            date_tmp = int(time.time())
            cntr = 0
            coin            = coin
            exchange        = exc
            countercurrency = "USD"
            date_from = int(time.time()) - 7*1440*60
            flag_coin_skip = False
            while date_tmp > date_from:
                cntr += 1
                try:
                    res=minute_price_historical(coin, countercurrency, toTs=date_tmp, limit=(60*24) ,aggregate=1, exchange=exchange)
                    if res[0]['close'].iloc[0] == 0:
                        break
                except:
                    # if no data found at all
                    if cntr == 1:
                        print("      No data for currency available, Skipping")
                    flag_coin_skip = True

                if flag_coin_skip == True:
                    break

                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                df = res[0]
                df = df[cols]
                date_tmp=df['time'].iloc[0]
                print(exchange + str(date_tmp))
                data = res[1]
                minute_data.update({'name': exchange, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
                print("Iteration Minute " + str(cntr) + ":\n")
                time.sleep(3)
            minute_data.update({'name': exchange, 'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)



    return 0



parse()
