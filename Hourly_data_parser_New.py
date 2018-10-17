
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
    f.close()
    for x in range(num_threads):
        worker = ProxyCheckWorker(proxyCheckQueue)
        worker.daemon = True
        worker.start()

    for proxy_info in proxy_list:
        proxyCheckQueue.put(proxy_info)

    proxyCheckQueue.join()

    print(len(valid_proxies))


# In[4]:

def daily_price_historical(symbol):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym=USD&allData=true'    .format(symbol.upper())
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[5]:

def hourly_price_historical(symbol, toTs, limit=100):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym=USD&limit={}&aggregate=1&toTs={}'    .format(symbol.upper(), limit, toTs)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[ ]:

def parse():

    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    coin_pairs = db.coin_pairs
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    cntr_d = 0

    coins = coin_pairs.distinct('Coin')
    proxy_cntr = 0
#     Daily data
    for coin in coins:
        try:

            cntr_d += 1
            res = daily_price_historical(coin)
            cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
            data = res[1]
            daily_data.update({'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
            print("Iteration Daily " + str(cntr_d) + ":\n")
            daily_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)
            time.sleep(1)
        except:
            next


    #Hourly data
    for coin in coins:
        date_tmp = int(time.time())
        cntr = 0

        date_from = int(time.mktime(datetime.datetime.strptime("2009-01-09-00:00:00","%Y-%m-%d-%H:%M:%S").timetuple()))
        flag_coin_skip = False
        while date_tmp > date_from:
            cntr += 1
            try:

                res=hourly_price_historical(coin, toTs=date_tmp, limit=(30*24))
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
            time.sleep(1)
        hourly_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)



    return 0


# In[7]:

parse()
