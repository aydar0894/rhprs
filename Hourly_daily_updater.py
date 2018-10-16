
# coding: utf-8

# In[1]:

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


# In[2]:

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
        self.mongo_c = None
        self.db_name = db_name
        self.mongo_c = self._mongo_connect()
        self.db = self.mongo_c[db_name]
        self.currencies_list  = currencies_list
        if currencies_list == 'all':
            self.currencies_list = self.db[return_frequency + "_data"].distinct('Ccy')
        self.time_points =  self.TIME_INTERVALS_DICT[return_frequency]
        if time_interval not in self.time_points:
            msg = 'Only %s values supports for %s collection' % (','.join(self.time_points),
                                                                 return_frequency)
            raise Exception(msg)
        self.return_frequency = "%s_data" % return_frequency
        self.currencies       = {}
#         print(self.db[self.return_frequency].distinct('Ccy'))


    def _preprocess_time_intervals(self):
        if self.return_frequency == 'daily_data':
            interval_days_sum  = 0
            now                = datetime.now()
            month_days         = lambda t: monthrange(t.year, t.month)[1]
            new_time_intervals = []
            prev_interval      = self.time_points[0]
            for interval in self.time_points[1:]:
                for month_delta in range(prev_interval, interval):
                    current = now - relativedelta(months=month_delta)
                    if current < self.start_time:
                        break
                    interval_days_sum += month_days(current)
                new_time_intervals.append(interval_days_sum)
                prev_interval = interval
            return new_time_intervals
        else:
            return self.time_points


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
        currencies_list  = deque(self.currencies_list)
        self._fix_currencies_time_bounds()
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


    def _fix_currencies_time_bounds(self):
        to_datetime = lambda x: datetime.fromtimestamp(x)
        self.start_time, self.end_time = map(to_datetime, self._return_time_bounds())
        if self.return_frequency == 'daily_data':
            self.start_time = self.start_time.replace(hour=0,minute=0,second=0)
            self.end_time   = self.end_time.replace(hour=0,minute=0,second=0)


    def _return_time_bounds(self):
        collection_data = self.db[self.return_frequency]
        minln = 0
        maxln = time.time()
        for data in collection_data.find({ 'Ccy': { '$in' : self.currencies_list } }):
            try:
                hist = data["history"]
                history = list(map(lambda x: x['time'], hist))
                if min(history) > minln:
                    minln = min(history)
                if max(history) < maxln:
                    maxln = max(history)
            except:
                next
        return (minln, maxln)


    def calculate_for_pair(self, benchmark_ccy, coin_ccy, last_n_times):
        # --- read coin ---
        arr_PnL_benchmark, arr_PnL_coin = self._timeseries(benchmark_ccy,
                                                           coin_ccy,
                                                           last_n_times)
        multiplier, correlation         = self._multiplier_and_correlation(arr_PnL_benchmark,
                                                                           arr_PnL_coin)
        return (multiplier, correlation)

    def _multiplier_and_correlation(self, arr_PnL_benchmark, arr_PnL_coin):
        #          calculate multiplier
        # least square regression (linear): y = alpha + beta*x
        linReg = np.polyfit(x=arr_PnL_benchmark, y=arr_PnL_coin, deg=1)
        alpha = linReg[1] # this is the y-intercept, not needed
        beta  = linReg[0] # this is the slope, which also is the multiplier
        multiplier = beta
        print("multiplier            : ", multiplier)
        #          calculate correlation          #
        correlation = pearsonr(arr_PnL_benchmark, arr_PnL_coin)
        print("correlation            :", correlation[0])
        return (multiplier, correlation[0])

    #-----------------------------------------#
    #          calculate return timeseries    #
    #-----------------------------------------#
    def _timeseries(self, benchmark_ccy, coin_ccy, last_n_times):
        df_benchmark = self._retrieve_currency_history(benchmark_ccy)
        df_benchmark = df_benchmark.pct_change()
#         print(df_benchmark)
        df_benchmark = df_benchmark['close'].values[1:last_n_times]
#         print(df_benchmark)

        df_coin      = self._retrieve_currency_history(coin_ccy).pct_change()
        df_coin      = df_coin['close'].values[1:last_n_times]

        return (df_benchmark, df_coin)


    def _increment_interval(self, *date_time_fields):
        if self.return_frequency == 'daily':
            return map(lambda dt: dt + timedelta(days=1), date_time_fields)
        elif self.return_frequency == 'hourly':
            return map(lambda dt: dt + timedelta(hours=1), date_time_fields)
        else:
            print('ERROR. Need to implment other frequencies')
            assert(False)

    # --- connect and preprocess utilities for mongo collection ---
    def _reconstruct_currency_date(self, cur):
        frmt = "{:%Y-%m-%d}"
        if self.return_frequency == 'hourly':
            frmt = "{:%Y-%m-%d %H:%M:%S}"
        for cur_value, index in zip(cur['history'], range(len(cur['history']))):
            #  cur['history'][index]['date'] = datetime.fromtimestamp(cur_value['time'])
            cur['history'][index]['date'] = frmt.format(datetime.fromtimestamp(cur_value['time']))
        return cur


    def _mongo_connect(self):
        if not self.mongo_c:
            self.mongo_c = MongoClient('localhost',
                    authSource=self.db_name)
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


# In[3]:

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


# In[4]:

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


# In[5]:

def hourly_price_historical(symbol):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym=USD&limit=1&aggregate=1'.format(symbol.upper())
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[6]:

def daily_price_historical(symbol):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym=USD&limit=1&aggregate=1'.format(symbol.upper())
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[10]:

def parse(dtype = "hourly"):

    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    coin_pairs = db.coin_pairs
    hourly_data = db.hourly_data
    daily_data = db.daily_data
    cntr_d = 0
    print("In parse")
    proxy_cntr = 0
#     Daily data
    if dtype == "daily":
        for data in coin_pairs.find():
            coin = data["Coin"]
            try:

                cntr_d += 1
                res = daily_price_historical(coin)
                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                data = res[1]
                daily_data.update({'Ccy': coin}, {'$push':  {'history': { '$each':data}}}, upsert=True)
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
        for data in hourly_data.find():
            print(data["Ccy"])


            coin = data["Ccy"]
            flag_coin_skip = False

            cntr += 1
            res=hourly_price_historical(coin)
#             try:


#             except:
#                 continue

#             try:
            cur_time = time.time()
            tmp_24 = hourly_data.find_one({'Ccy': 'BTC'} , {'history' :  {'$elemMatch' :{'time' : {'$gte': cur_time - 60*60*25, '$lte' : cur_time - 60*60*23}}}})
            tmp_7d = hourly_data.find_one({'Ccy': 'BTC'} , {'history' :  {'$elemMatch' :{'time' : {'$gte': cur_time - 7*60*60*25, '$lte' : cur_time - 7*60*60*23}}}})
            tmp_30d = hourly_data.find_one({'Ccy': 'BTC'} , {'history' :  {'$elemMatch' :{'time' : {'$gte': cur_time - 30*60*60*25, '$lte' : cur_time - 30*60*60*23}}}})
            dat = res[1][1]
            change_24 = dat['close'] - tmp_24['history'][0]['close']
            change_7d = dat['close'] - tmp_7d['history'][0]['close']
            change_30d = dat['close'] - tmp_30d['history'][0]['close']
            data['change_24'] = change_24
            data['change_7d'] = change_7d
            data['change_30d'] = change_30d
#             except:
#                 continue
            cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
            data = res[1][1]
            hourly_data.update({'Ccy': coin}, {'$push':  {'history': data}}, upsert=True)
            print("Iteration Hourly " + str(cntr) + ":\n")
            time.sleep(2)
            hourly_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time(), 'price': dat['close'], 'change_24' : change_24, 'change_7d' : change_7d, 'change_30d' : change_30d}}, upsert=True)

#         new_compare = MultiplierCorellationCalculator(
#                     time_interval=1,
#                     currencies_list='all',
#                     return_frequency='hourly')
#         new_compare.recalculate_pairs()



# In[11]:

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
            parse(dtype="daily")
            last_daily_update = time.time()

        if (time.time() - last_hourly_update) >= 60*60:
            parse(dtype="hourly")
            last_hourly_update = time.time()
        else:
            print("Wating until next iteration")
            time.sleep(3600 - (time.time() - last_hourly_update))


# In[12]:

main()
