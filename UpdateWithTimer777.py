
# coding: utf-8

# In[2]:

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
from queue import Queue
from threading import Thread

list_coins = ["BTC", "ETH", "LTC", "ETC", "XRP", "XMR"]
list_exchanges = ["Kraken", "Poloniex", "Bitfinex", "Huobi", "Bitstamp", "LocalBitcoins", "Cryptsy", "BitBay", "BitTrex", "Exmo"]


# In[3]:

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


# In[4]:

class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            data = self.queue.get()
            data_type = data
            if data_type == 1:
                run_minute()
            if data_type == 2:
                run_hourly()
            if data_type == 3:
                run_daily()
            self.queue.task_done()


# In[5]:

def run_minute():
    i = 0
    while True:
        i+=1
        time.sleep(60)
        parse(1, 1)
        print("M " + str(i))


# In[6]:

def run_hourly():
    i = 0
    while True:
        i+=1
        time.sleep(60*60)
        parse(1, 2)
        print("H " + str(i))


# In[7]:

def run_daily():
    i = 0
    while True:
        i+=1
        time.sleep(60*60*24)
        parse(1, 3)
        print("D " + str(i))


# In[8]:

def hourly_price_historical(symbol, comparison_symbol, limit=1, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym={}&limit={}&aggregate={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[9]:

def daily_price_historical(symbol, comparison_symbol, limit=1, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&limit={}'    .format(symbol.upper(), comparison_symbol.upper(), limit)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    print(url)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[10]:

def minute_price_historical(symbol, comparison_symbol, limit=1, aggregate=1, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[11]:

def parse(lim, data_type):
    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    cntr = 0

    #Daily data
    if data_type == 3:
        for exc in list_exchanges:
            for coin in list_coins:
                cntr += 1
                countercurrency = "USD"
                res = daily_price_historical(coin, countercurrency, limit=lim, exchange=exc)
                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                data = res[1]
                daily_data.update({'name': exc, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
                print("Iteration Daily " + str(cntr) + ":\n")
                daily_data.update({'name': exc, 'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)
                time.sleep(3)

    #Hourly data
    if data_type == 2:
        for exc in list_exchanges:
            for coin in list_coins:

                countercurrency = "USD"
                cntr += 1

                try:
                    res=hourly_price_historical(coin, countercurrency, limit=lim, exchange=exc)
                    if res[0]['close'].iloc[0] == 0:
                        next
                except:
                    # if no data found at all
                    if cntr == 1:
                        print("      No data for currency available, Skipping")
                    next

                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                data = res[1]
                hourly_data.update({'name': exc, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
                print("Iteration Hourly " + str(cntr) + ":\n")
                time.sleep(3)
                hourly_data.update({'name': exc, 'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)

    #Minute data
    if data_type == 1:
        for exc in list_exchanges:
            for coin in list_coins:

                countercurrency = "USD"

                cntr += 1

                try:
                    res=minute_price_historical(coin, countercurrency, limit=lim, exchange=exc)
                    if res[0]['close'].iloc[0] == 0:
                        next
                except:
                    # if no data found at all
                    if cntr == 1:
                        print("      No data for currency available, Skipping")
                    next



                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']

                data = res[1]
                minute_data.update({'name': exc, 'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
                print("Iteration Minute " + str(cntr) + ":\n")
                minute_data.update({'name': exc, 'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)
            time.sleep(3)


# In[12]:

def main():
    queue = Queue()

    for x in range(4):
        worker = DownloadWorker(queue)
        worker.daemon = False
        worker.start()

    last_updates = get_timestamps("Kraken")

    timestamp = time.time()


    if(timestamp >= last_updates[2] + 60*60*24):
        limit = int((timestamp - last_updates[2])/(60*60*24))
        print(limit)
        parse(lim = limit, data_type = 3)

    if(timestamp >= last_updates[1] + 60*60):
        limit = int((timestamp - last_updates[1])/(60*60))
        parse(lim = limit, data_type = 2)

    if(timestamp >= last_updates[0] + 60):
        limit = int((timestamp - last_updates[0])/60)
        parse(lim = limit, data_type = 1)

    for i in range(3):
        queue.put(i + 1)


# In[13]:

main()


# In[ ]:
