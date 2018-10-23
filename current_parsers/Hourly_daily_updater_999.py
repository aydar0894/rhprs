
# coding: utf-8

# In[11]:

import cryptocompare
import requests
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time
from pymongo import MongoClient
import datetime
import dns
import sys
import os
import websocket
from queue import Queue
from threading import Thread
import urllib.request as urllib
from pprint import pprint
from scipy.stats.stats import pearsonr
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from enum import Enum
import copy
from collections import deque

valid_proxies = []
proxyCheckQueue = Queue()



# In[33]:

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


# In[26]:

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


# In[27]:

def hourly_price_historical(symbol, limit=1):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym=USD&limit={}&aggregate=1'.format(symbol.upper(), int(limit))
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
#     pprint(page)
    df['timestamp'] = [datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[28]:

def daily_price_historical(symbol):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym=USD&limit=1&aggregate=1'.format(symbol.upper())
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


def get_market_caps(coin_list):
    coins_caps = {}
    for i in range(8):
        url = 'https://api.coinmarketcap.com/v2/ticker/?start={}&limit={}&sort=rank'.format(i*100+1, 100)
        page = requests.get(url)
        data = page.json()['data']
        # pprint(data)
        for index in data:
            if data[index]['symbol'] in coin_list:
                coins_caps.update({data[index]['symbol']: {'market_cap' : data[index]['quotes']['USD']['market_cap'], 'rank' : data[index]['rank']}})
                time.sleep(0.5)
    return(coins_caps)

# In[29]:

def parse(dtype = "hourly"):

    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    coin_pairs = db.coin_pairs
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    cntr_d = 0
    print("In parse")
    daily_coins = daily_data.distinct('Ccy')
    hourly_coins = hourly_data.distinct('Ccy')

    proxy_cntr = 0
#     Daily data

    if dtype == "daily":
        for coin in daily_coins:
            daily_data.update({'Ccy': coin}, {'$push':  {'history' : { '$each': [], '$sort' : {'time' : -1} }}}, upsert=True)
            try:

                cntr_d += 1
                res = daily_price_historical(coin)
                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                data = res[1]
                daily_data.update({'Ccy': coin}, {'$push':  {'history' : {'$each': data, '$position': 0}}}, upsert=True)
                print("Iteration Daily " + str(cntr_d) + ":\n")
                daily_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)
                time.sleep(3)
            except:
                next


    #Hourly data
    if dtype == "hourly":
        market_caps = get_market_caps(hourly_coins)
        cntr = 0
        for coin in hourly_coins:

            flag_coin_skip = False
            cntr += 1
            last_upd = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : 1}})
            try:
                last_upd = last_upd['history'][0]
                limit = (time.time() - last_upd['time'])/(60*60)
            except:
                limit = 2000

            try:
                res=hourly_price_historical(coin, int(limit - 1))
            except:
                print("No data available")
                continue
            pprint(res[1])
#             pprint(len(res[1]))
            cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']


            if len(res[1]) > 2:
                dat = res[1]
                data = res[1]
                hourly_data.update({'Ccy': coin}, {'$push':  {'history' : {'$each': data, '$position': 0}}}, upsert=True)
            else:
                dat = res[1][1]
                data = res[1][1]
                hourly_data.update({'Ccy': coin}, {'$push':  {'history' : {'$each': [data], '$position': 0}}}, upsert=True)



            hourly_data.update({'Ccy': coin}, {'$push':  {'history' : { '$each': [], '$sort' : {'time' : -1} }}}, upsert=True)

            print("Iteration Hourly " + str(cntr) + ":\n")

            last_upd = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : 1}})
            last_upd = last_upd['history'][0]

            cur_time = time.time()
            tmp_24 = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : 24}})
            tmp_7d = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : (7*24)}})
            tmp_30d = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : (30*24)}})


            change_24 = tmp_24['history'][0]['close'] - tmp_24['history'][len(tmp_24['history'])-1]['close']
            change_7d = tmp_7d['history'][0]['close'] - tmp_7d['history'][len(tmp_7d['history'])-1]['close']
            change_30d = tmp_30d['history'][0]['close'] - tmp_30d['history'][len(tmp_30d['history'])-1]['close']

            try:
                change_24_pc = (tmp_24['history'][0]['close']/tmp_24['history'][len(tmp_24['history'])-1]['close'] - 1) * 100
            except:
                change_24_pc = 100
            try:
                change_7d_pc = (tmp_7d['history'][0]['close']/tmp_7d['history'][len(tmp_7d['history'])-1]['close'] - 1) * 100
            except:
                change_7d_pc = 100
            try:
                change_30d_pc = (tmp_30d['history'][0]['close']/tmp_30d['history'][len(tmp_30d['history'])-1]['close'] - 1) * 100
            except:
                change_30d_pc = 100

            vot_tmp = hourly_data.find_one({'Ccy': coin})
            df_data1 = pd.DataFrame(vot_tmp['history'][:365*24])
            cl = df_data1.pct_change()
            close = cl['close'][1:]
            vol = np.std(close)


            try:
                marcap = market_caps[coin]['market_cap']
                rank = market_caps[coin]['rank']

            except:
                marcap = 0
                rank = 0

            try:
                prev_mc = hourly_data.find_one({'Ccy': coin})
                prev_mc = prev_mc['market_cap']
                marcap = market_caps[coin]['market_cap']
                marcap_change = marcap - prev_mc
                marcap_change_pc = (marcap/prev_mc - 1) * 100
                
            except:
                marcap_change = 0
                marcap_change_pc = 0

            hourly_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time(), 'price': res[1][len(res[1])-1]['close'], 'market_cap': marcap, 'rank': rank, 'market_cap_change': marcap_change, 'market_cap_change_pc': marcap_change_pc,  'volatility': vol, 'change_24' : change_24, 'change_7d' : change_7d, 'change_30d' : change_30d, 'change_24_pc' : change_24_pc, 'change_7d_pc' : change_7d_pc, 'change_30d_pc' : change_30d_pc}}, upsert=True)

            time.sleep(3)



# In[30]:

def main():
    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    dd = daily_data.find_one({'Ccy': 'BTC'})
    hd = hourly_data.find_one({'Ccy': 'BTC'})
    last_daily_update = dd["last_update"]
    last_hourly_update = hd["last_update"]
    while True:


        if (time.time() - last_daily_update) >= 60*60*24:
            last_daily_update = time.time()
            parse(dtype="daily")


        if (time.time() - last_hourly_update) >= 60*60:
            last_hourly_update = time.time()
            parse(dtype="hourly")

        else:
            print("Wating until next iteration")
            time.sleep(3600 - (time.time() - last_hourly_update))


# In[31]:

main()
