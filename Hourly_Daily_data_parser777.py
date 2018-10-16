
# coding: utf-8

# In[1]:

# db.usd_courses_history.updateOne({name: "Gdax"}, {$addToSet:  { history:{$each: [{e: 5, c: 4}]}}});
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
from threading import Thread
from queue import Queue
import urllib.request as urllib
valid_proxies = []
proxyCheckQueue = Queue()

# list_coin_exchange_pairs = [ \
#                              ["BTC",    "Kraken",     "USD"]  ,\
#                              ["BTC",    "Poloniex",   "USD"]  ,\
#                              ["BTC",    "Bitfinex",   "USD"]  ,\
#                              ["BTC",    "Huobi",     "USD"]  ,\
#                              ["BTC",    "Bitstamp",   "USD"]  ,\
#                              ["BTC",    "Exmo",   "USD"]  ,\
#                              ["BTC",    "LocalBitcoins",     "USD"]  ,\
#                              ["BTC",    "Cryptsy",   "USD"]  ,\
#                              ["BTC",    "BitBay",   "USD"]  ,\
#                              ["BTC",    "BitTrex",    "USD"]
#                            ]


# In[2]:

class ProxyCheckWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            proxy_info = self.queue.get()
            proxy_handler = urllib.ProxyHandler({'http': proxy_info})
            opener = urllib.build_opener(proxy_handler)
            opener.addheaders = [('User-agent','Mozilla/5.0')]
            urllib.install_opener(opener)
            req = urllib.Request("http://www.google.com")
            try:
                sock=urllib.urlopen(req, timeout= 0.5)
                rs = sock.read(1000)
                if '<title>Google</title>' in str(rs):
                    valid_proxies.append(proxy_info)
            except:
                pass


            self.queue.task_done()


# In[3]:

def check_proxies(proxy_list_path, num_threads):
    result = []
    start = time.time()
    f = open(proxy_list_path, "r")
    proxy_list = f.read().split('\n')[:-1]
    for x in range(num_threads):
        worker = ProxyCheckWorker(proxyCheckQueue)
        worker.daemon = True
        worker.start()

    for proxy_info in proxy_list:
        proxyCheckQueue.put(proxy_info)

    proxyCheckQueue.join()

    print(len(valid_proxies))


# In[4]:

# from requests.exceptions import ConnectionError

# # extra function to support XHR1 style protocol
# def _new_read_packet_length(content, content_index):
#     packet_length_string = ''
#     while get_byte(content, content_index) != ord(':'):
#         byte = get_byte(content, content_index)
#         packet_length_string += chr(byte)
#         content_index += 1
#     content_index += 1
#     return content_index, int(packet_length_string)

# def new_decode_engineIO_content(content):
#     content_index = 0
#     content_length = len(content)
#     while content_index < content_length:
#         try:
#             content_index, packet_length = _new_read_packet_length(
#                 content, content_index)
#         except IndexError:
#             break
#         content_index, packet_text = _read_packet_text(
#             content, content_index, packet_length)
#         engineIO_packet_type, engineIO_packet_data = parse_packet_text(
#             packet_text)
#         yield engineIO_packet_type, engineIO_packet_data

# def new_recv_packet(self):
#     params = dict(self._params)
#     params['t'] = self._get_timestamp()
#     response = get_response(
#         self.http_session.get,
#         self._http_url,
#         params=params,
#         **self._kw_get)
#     for engineIO_packet in new_decode_engineIO_content(response.content):
#         engineIO_packet_type, engineIO_packet_data = engineIO_packet
#         yield engineIO_packet_type, engineIO_packet_data

# setattr(socketIO_client.transports.XHR_PollingTransport, 'recv_packet', new_recv_packet)


# In[5]:

def daily_price_historical(symbol, comparison_symbol, proxy = {'https' : 'https://54.39.144.247:3128'}):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&allData=true'    .format(symbol.upper(), comparison_symbol.upper())
#     if exchange:
#         url += '&e={}'.format(exchange)
    page = requests.get(url, proxies=proxy, timeout=5)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[6]:

def hourly_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange='', proxy = {'https' : 'https://54.39.144.247:3128'}):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym={}&limit={}&aggregate={}&toTs={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate, toTs)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[7]:

def minute_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}&toTs={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate, toTs)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[8]:

def parse():
    del valid_proxies[:]
    proxy_list_path = "proxies.txt"
    check_proxies(proxy_list_path, 20)

    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    cntr_d = 0

    f = open("top50_coins.txt", "r")
    list_coins = f.read().split('\n')[:-1]
    proxy_cntr = 0
    # Daily data
    for coin in list_coins:
        try:
            if proxy_cntr == len(valid_proxies) - 1:
                proxy_cntr = 0
            proxy = valid_proxies[proxy_cntr]
            proxy_cntr += 1
            proxy_obj = {'https' : 'https://' + proxy}

            cntr_d += 1
            coin            = coin
            countercurrency = "USD"
            res = daily_price_historical(coin, countercurrency, proxy_obj)
            cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
            data = res[1]
            daily_data.update({'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
            print("Iteration Daily " + str(cntr_d) + ":\n")
            daily_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)
            time.sleep(1)
        except:
            next


    #Hourly data
    for coin in list_coins:
        date_tmp = int(time.time())
        cntr = 0
        coin            = coin
        countercurrency = "USD"


        date_from = int(time.mktime(datetime.datetime.strptime("2009-01-09-00:00:00","%Y-%m-%d-%H:%M:%S").timetuple()))
        flag_coin_skip = False
        while date_tmp > date_from:
            cntr += 1
            try:
                if proxy_cntr == len(valid_proxies) - 1:
                    proxy_cntr = 0
                proxy = valid_proxies[proxy_cntr]
                proxy_cntr += 1
                proxy_obj = {'https' : 'https://' + proxy}

                res=hourly_price_historical(coin, countercurrency, toTs=date_tmp, limit=(30*24) ,aggregate=1)
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
            hourly_data.update({'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
            print("Iteration Hourly " + str(cntr) + ":\n")
            time.sleep(3)
        hourly_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)

    #Minute data
#     for coin in list_coins:
#         date_tmp = int(time.time())
#         cntr = 0
#         coin            = coin
#         countercurrency = "USD"
#         date_from = int(time.time()) - 7*1440*60
#         flag_coin_skip = False
#         while date_tmp > date_from:
#             cntr += 1
#             try:
#                 res=minute_price_historical(coin, countercurrency, toTs=date_tmp, limit=(60*24) ,aggregate=1)
#                 if res[0]['close'].iloc[0] == 0:
#                     break
#             except:
#                 # if no data found at all
#                 if cntr == 1:
#                     print("      No data for currency available, Skipping")
#                 flag_coin_skip = True

#             if flag_coin_skip == True:
#                 break

#             cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
#             df = res[0]
#             df = df[cols]
#             date_tmp=df['time'].iloc[0]
#             print(exchange + str(date_tmp))
#             data = res[1]
#             minute_data.update({'name': exchange, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
#             print("Iteration Minute " + str(cntr) + ":\n")
#             time.sleep(3)
#         minute_data.update({'name': exchange, 'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)



    return 0


# In[9]:

parse()


# In[3]:

f = open("top50_coins.txt", "r")
list_coins = f.read().split('\n')[:-1]
print(list_coins)


# In[86]:

def on_subadd_response(*args):
    print(args)
    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    usd_courses_history = db.usd_courses_history

def on_connect():
    print("Opened")
def on_error():
    print("error")
def on_disconnect():
    print("Closed")


# In[177]:

socketIO = socketIO_client.SocketIO('https://streamer.cryptocompare.com')
socketIO.on('connect', on_connect)
socketIO.on('disconnect', on_disconnect)
socketIO.on('m', on_subadd_response)
socketIO.emit('SubAdd',{ 'subs': ['2~Poloniex~BTC~USD']})
socketIO.wait()


# In[98]:

with SocketIO('https://streamer.cryptocompare.com', 443, LoggingNamespace) as socketIO:
    socketIO.on('connect', on_connect)
    socketIO.on('disconnect', on_disconnect)
    socketIO.emit('SubAdd',{ 'subs': ['0~Poloniex~BTC~USD']})
    socketIO.wait()


# In[ ]:
