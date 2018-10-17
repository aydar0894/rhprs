
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


# In[32]:

from flask import Flask
from flask import request
import numpy as np
import pandas as pd
from scipy.stats.stats import pearsonr # used to calculate correlation coefficient
from pymongo import MongoClient
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from pprint import pprint
from enum import Enum
import copy
from collections import deque
import time

class MultiplierCorellationCalculator:
    class RequestFrequency(Enum):
        DAILY  = 0
        HOURLY = 1

    class HourlyTimeIntervals(Enum):
        A_DAY       = 1
        FIVE_DAYS   = 5
        WEEK        = 7
        TEN_DAYS    = 10
        TWO_WEEKS   = 14

    class DailyTimeIntervals(Enum):
        A_MONTH      = 1
        THREE_MONTHS = 3
        HALF_YEAR    = 6
        NINE_MONTHS  = 9
        A_YEAR       = 12

    FREQUENCY_LIST        = RequestFrequency.__members__.keys()
    HOURLY_TIME_INTERVALS = list(map(lambda x: x.value, HourlyTimeIntervals.__members__.values()))
    DAILY_TIME_INTERVALS  = list(map(lambda x: x.value, DailyTimeIntervals.__members__.values()))
    TIME_INTERVALS_DICT   = {
        'hourly': HOURLY_TIME_INTERVALS,
        'daily': DAILY_TIME_INTERVALS,
    }

    def __init__(self,
                 db_name='bitcoin',
                 time_interval=1,
                 currencies_list='all',
                 return_frequency='daily'):
        if return_frequency.upper() not in self.FREQUENCY_LIST:
            raise Exception('Only [daily, hourly] values supports for return_frequency parameter yet...')
        self.time_points =  self.TIME_INTERVALS_DICT[return_frequency]
        if time_interval not in self.time_points:
            msg = 'Only %s values supports for %s collection' % (','.join(self.time_points),
                                                                 return_frequency)
            raise Exception(msg)
        self.return_frequency = "%s_data_test" % return_frequency    # select correct collection name
        self.mongo_c = None
        self._mongo_connect(db_name)
        self.db = self.mongo_c[db_name]
        self.currencies_list  = currencies_list
        self.collection       = self.db[self.return_frequency]
        if currencies_list == 'all':
            self.currencies_list = [x['Ccy'] for x in self.collection.find({},{'Ccy': 1, '_id': 0})]
        self._calculate_time_bounds()
        self.currencies       = {}


    def _preprocess_time_intervals(self):
        if self.return_frequency == 'daily_data_test':
            return list(map(lambda x: x * 30, self.time_points))
        else:
            return list(map(lambda x: x * 24, self.time_points))


    def recalculate_pairs(self):
        """
        Example for daily:

        'm_and_c_matrix': {
            '1': [  {   ccy: 'ETH',
                        multiplier: 0.5,
                        correlation: 0.93 },
                    {   ccy: 'LTC',
                        mult: 0.5,
                        corr: 0.93 }
            ],
            '5': [  {   ccy: 'ETH',
                        multiplier: 0.5,
                        correlation: 0.93 },
                    {   ccy: 'LTC',
                        mult: 0.5,
                        corr: 0.93 }
            ],
            ......
            '14': [  {   ccy: 'ETH',
                        multiplier: 0.5,
                        correlation: 0.93 },
                    {   ccy: 'LTC',
                        mult: 0.5,
                        corr: 0.93 }
            ]
        }
        """
        pprint("In recalculate pairs")
        currencies_list  = deque(self.currencies_list)
        time_intervals   = self._preprocess_time_intervals()
        pairs_multiplier_correlation = {}
        for benchmark_currency in currencies_list:
            matrix = {}
            for n_times, point in zip(time_intervals, self.time_points):
                pairs = []
                for coin_currency in [x for x in currencies_list if x != benchmark_currency]:
                    multiplier, correlation = self.calculate_for_pair(
                                                benchmark_currency,
                                                coin_currency,
                                                n_times)
                    pair = { 'ccy': coin_currency,
                             'multiplier': multiplier,
                             'correlation': correlation }
                    pairs.append(pair)
                matrix[str(point)] = pairs

            self.db[self.return_frequency].update(
               { 'Ccy': benchmark_currency },
               {
                 '$set': { "m_and_c_matrix": matrix}
               },
                upsert=True
            )


    def _calculate_time_bounds(self):
        pipeline = [
            {
                '$unwind': "$history"
            },
            {
                '$group' :
                {
                    '_id': "$Ccy",
                    'max': { '$max' : "$history.time" },
                    'min': { '$min' : "$history.time" }
                }
            }
        ]
        self.currencies_min_max_time = { x['_id']: {'min':x['min'], 'max':x['max']} for x in self.collection.aggregate(pipeline)}


    def _fix_horizon(self, benchmark_ccy, coin_ccy, horizon):
        to_datetime = lambda x: datetime.fromtimestamp(x)
        print("Bench %s Coin %s" % (benchmark_ccy, coin_ccy))
        print(self.currencies_min_max_time)
        bc_min = max(self.currencies_min_max_time[benchmark_ccy]['min'],
                     self.currencies_min_max_time[coin_ccy]['min'])
        bc_max = min(self.currencies_min_max_time[benchmark_ccy]['max'],
                     self.currencies_min_max_time[coin_ccy]['max'])
        minln = to_datetime(bc_min)
        maxln = to_datetime(bc_max)
        delta = maxln - minln
        n_times = horizon
        if self.return_frequency == 'daily_data_test':
            delta = delta.days
        else:
            delta = int(delta.seconds/3600) + delta.days*24
        if delta > 1 and delta < n_times:
            n_times = delta

        if horizon != n_times:
            print("Horizon for %s/%s pair fixed from %s to %s" % (benchmark_ccy, coin_ccy, horizon, n_times))
        return n_times


    def calculate_for_pair(self, benchmark_ccy, coin_ccy, horizon):
        # --- read coin ---
        fixed_horizon                   = self._fix_horizon(benchmark_ccy,
                                                            coin_ccy,
                                                            horizon)
        arr_PnL_benchmark, arr_PnL_coin = self._timeseries(benchmark_ccy,
                                                           coin_ccy,
                                                           fixed_horizon)
        multiplier, correlation         = self._multiplier_and_correlation(arr_PnL_benchmark,
                                                                           arr_PnL_coin)
        return (multiplier, correlation)

    def _multiplier_and_correlation(self, arr_PnL_benchmark, arr_PnL_coin):
        #          calculate multiplier
        # least square regression (linear): y = alpha + beta*x
        try:
            linReg = np.polyfit(x=arr_PnL_benchmark, y=arr_PnL_coin, deg=1)
            alpha = linReg[1] # this is the y-intercept, not needed
            beta  = linReg[0] # this is the slope, which also is the multiplier
        except:
            alpha = 0.5
            beta = 0.5
        multiplier = beta
        print("multiplier            : ", multiplier)
        #          calculate correlation          #
        try:
            correlation = pearsonr(arr_PnL_benchmark, arr_PnL_coin)
        except:
            correlation = 0.5
        print("correlation            :", correlation[0])
        return (multiplier, correlation[0])

    #-----------------------------------------#
    #          calculate return timeseries    #
    #-----------------------------------------#
    def _timeseries(self, benchmark_ccy, coin_ccy, horizon):
        df_benchmark = self._retrieve_currency_history(benchmark_ccy)
        df_benchmark = df_benchmark.pct_change()
        df_benchmark = df_benchmark['close'].values[1:horizon]
#         print(df_benchmark)

        df_coin      = self._retrieve_currency_history(coin_ccy).pct_change()
        df_coin      = df_coin['close'].values[1:horizon]

        return (df_benchmark, df_coin)


    # --- connect and preprocess utilities for mongo collection ---
    def _reconstruct_currency_date(self, cur):
        frmt = "{:%Y-%m-%d}"
        if self.return_frequency == 'hourly':
            frmt = "{:%Y-%m-%d %H:%M:%S}"
        for cur_value, index in zip(cur['history'], range(len(cur['history']))):
            #  cur['history'][index]['date'] = datetime.fromtimestamp(cur_value['time'])
            cur['history'][index]['date'] = frmt.format(datetime.fromtimestamp(cur_value['time']))
        return cur


    def _mongo_connect(self, db_name):
        if self.mongo_c == None:
            self.mongo_c = MongoClient('localhost',
                    authSource=db_name)
        return self.mongo_c


    def _preprocess_collection(self, collection_name, filter_params):
        collection = self.db[collection_name]
        if not collection:
            raise Exception('collection not found')
        return self._reconstruct_currency_date(collection.find_one(filter_params))


    def _retrieve_currency_history(self, currency):
        if currency not in self.currencies:
            collection_schema = self.return_frequency # return frequency points to the name of collection
            df_data = self._preprocess_collection(collection_schema, {'Ccy': currency})
            df_data = pd.DataFrame(df_data['history'])
            # this makes indexing via date faster
            df_data = df_data.set_index(['date'])         # index: string
            df_data.index = pd.to_datetime(df_data.index)
            self.currencies[currency] = df_data
        return self.currencies[currency]


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

    if dtype == "daily":
        for coin in daily_coins:
            daily_data.update({'Ccy': coin}, {'$push':  {'history' : { '$each': [], '$sort' : -1 }}}, upsert=True)
            try:

                cntr_d += 1
                res = daily_price_historical(coin)
                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                data = res[1]
                daily_data.update({'Ccy': coin}, {'$push':  {'history' : {'$each': data, '$position': 0}}}, upsert=True)
                print("Iteration Daily " + str(cntr_d) + ":\n")
                daily_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)
                time.sleep(2)
            except:
                next
        new_compare = MultiplierCorellationCalculator(
                    time_interval=1,
                    currencies_list='all',
                    return_frequency='daily')
        new_compare.recalculate_pairs()


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

        new_compare = MultiplierCorellationCalculator(
                    time_interval=1,
                    currencies_list='all',
                    return_frequency='hourly')
        new_compare.recalculate_pairs()



# In[30]:

def main():
    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    hourly_data = db.hourly_data_test
    daily_data = db.daily_data_test
    dd = daily_data.find_one({'Ccy': 'BTC'})
    hd = hourly_data.find_one({'Ccy': 'BTC'})
    last_daily_update = dd["last_update"]
    last_hourly_update = hd["last_update"]
    while True:


        if (time.time() - last_daily_update) >= 60*60*24:
            parse(dtype="daily")
            last_daily_update = time.time()

        if (time.time() - last_hourly_update) >= 60*60:
            parse(dtype="hourly")
            last_hourly_update = time.time()
        else:
            print("Wating until next iteration")
            time.sleep(3600 - (time.time() - last_hourly_update))


# In[31]:

main()
