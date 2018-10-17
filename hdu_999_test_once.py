
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


# In[29]:

def parse(dtype = "hourly"):

    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    coin_pairs = db.coin_pairs
    hourly_data = db.hourly_data_test
    daily_data = db.daily_data_test
    cntr_d = 0
    print("In parse")
    daily_coins = daily_data.distinct('Ccy')
    hourly_coins = hourly_data.distinct('Ccy')

    proxy_cntr = 0
#     Daily data

        #Hourly data
    if dtype == "hourly":
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
            else:
                dat = res[1][1]
                data = res[1][1]



            hourly_data.update({'Ccy': coin}, {'$push':  {'history' : { '$each': [], '$sort' : {'time' : -1} }}}, upsert=True)

            print("Iteration Hourly " + str(cntr) + ":\n")

            last_upd = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : 1}})
            last_upd = last_upd['history'][0]

            cur_time = time.time()
            tmp_24 = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : 24}})
            tmp_7d = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : (7*24)}})
            tmp_30d = hourly_data.find_one({'Ccy': coin} , {'history' :  {'$slice' : (30*24)}})

            if len(res[1]) > 2:
                change_24 = dat[1]['close'] - tmp_24['history'][len(tmp_24['history'])-1]['close']
                change_7d = dat[1]['close'] - tmp_7d['history'][len(tmp_7d['history'])-1]['close']
                change_30d = dat[1]['close'] - tmp_30d['history'][len(tmp_30d['history'])-1]['close']

            else:
                change_24 = dat['close'] - tmp_24['history'][0]['close']
                change_7d = dat['close'] - tmp_7d['history'][0]['close']
                change_30d = dat['close'] - tmp_30d['history'][0]['close']

            vot_tmp = hourly_data.find_one({'Ccy': coin})
            df_data1 = pd.DataFrame(vot_tmp['history'][:365*24])
            cl = df_data1.pct_change()
            close = cl['close'][1:]
            vol = np.std(close)


            hourly_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time(), 'price': res[1][len(res[1])-1]['close'], 'volatility': vol, 'change_24' : change_24, 'change_7d' : change_7d, 'change_30d' : change_30d}}, upsert=True)

            time.sleep(2)
            #             try:


            #             except:
            #                 continue

            #             try:


                        #             dat = res[1][1]

            #             except:
            #                 continue



# In[30]:

def main():
    parse(dtype="hourly")



main()
