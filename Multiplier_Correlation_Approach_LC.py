
# # Multiplier and Correlation class calculator
#
# Class calculates multiplier and correlation matrix
#

# In[11]:

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
        self.return_frequency = "%s_data" % return_frequency    # select correct collection name
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
        if self.return_frequency == 'daily_data':
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
        if self.return_frequency == 'daily_data':
            delta = delta.days
        else:
            delta = delta.seconds / 360

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
    def _timeseries(self, benchmark_ccy, coin_ccy, horizon):
        df_benchmark = self._retrieve_currency_history(benchmark_ccy)
        df_benchmark = df_benchmark.pct_change()
        print(df_benchmark['close'])
        df_benchmark = df_benchmark['close'].values[-horizon:-1]
#         print(df_benchmark)

        df_coin      = self._retrieve_currency_history(coin_ccy).pct_change()
        df_coin      = df_coin['close'].values[-horizon:-1]

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




# In[12]:

time_interval = 1

return_frequency = 'daily'

new_compare = MultiplierCorellationCalculator(
                time_interval=time_interval,
                currencies_list='all',
                return_frequency=return_frequency)
new_compare.recalculate_pairs()
