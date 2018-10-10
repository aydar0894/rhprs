
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

valid_proxies = []
proxyCheckQueue = Queue()


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


# In[8]:

def hourly_price_historical(symbol):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym=USD&limit=1&aggregate=1'    .format(symbol.upper())
    
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[9]:

def daily_price_historical(symbol):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym=USD&limit=1&aggregate=1'    .format(symbol.upper())
    
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[10]:

def parse(dtype = "hourly"):
    
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
    
    if dtype == "daily":
    #Daily data
        for coin in list_coins:
            try:          
                cntr_d += 1           
                res = daily_price_historical(coin)
                cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                data = res[1][1]
                daily_data.update({'Ccy': coin}, {'$addToSet':  {'history': data}}, upsert=True)
                print("Iteration Daily " + str(cntr_d) + ":\n")
                daily_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)
                time.sleep(1)
            except:
                next

    if dtype == "hourly":
    #Hourly data
        for coin in list_coins:
            cntr = 0        
            coin            = coin 
            flag_coin_skip = False

            cntr += 1
            try:
                res=hourly_price_historical(coin)

            except:
                # if no data found at all
                if cntr == 1:
                    print("      No data for currency available, Skipping")
                flag_coin_skip = True

            if flag_coin_skip == True:
                break

            cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']         
            data = res[1][1]
            hourly_data.update({'Ccy': coin}, {'$addToSet':  {'history': data}}, upsert=True)
            print("Iteration Hourly " + str(cntr) + ":\n")
            time.sleep(3)

            hourly_data.update({'Ccy': coin}, {'$set':  {'last_update': time.time()}}, upsert=True)


# In[11]:

def main():
    client = MongoClient('localhost',
                    authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data    
    hourly_data = db.hourly_data
    daily_data = db.daily_data  
    
    while True:
        dd = daily_data.find_one({'Ccy': 'GNT'})
        hd = hourly_data.find_one({'Ccy': 'GNT'})
        last_daily_update = dd["last_update"]
        last_hourly_update = hd["last_update"]    
        
#         print((time.time() - last_hourly_update))
        if (time.time() - last_daily_update) >= 60*60*24:
            parse(dtype="daily")            
        elif (time.time() - last_hourly_update) >= 60*60:
            parse(dtype="hourly")
        else:
            print("Wating until next iteration")
            time.sleep(3600 - (time.time() - last_hourly_update))


# In[12]:

main()


# In[ ]:



