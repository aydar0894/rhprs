
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
from queue import Queue
from threading import Thread
import urllib.request as urllib
valid_proxies = []
proxyCheckQueue = Queue()
dataUpdateQueue = Queue()


# In[2]:

class DataUpdateWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:           
            info = self.queue.get()
            coin = info["coin"]
            countercurrency = info["countercurrency"]            
            prox = info["proxy"]
            exchange = info["exchange"]


            print(info)
            minute_update_one(coin, countercurrency, prox, exchange)
            self.queue.task_done()


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
                sock=urllib.urlopen(req, timeout= 1.5)
                rs = sock.read(1000)
                if '<title>Google</title>' in str(rs):
                    valid_proxies.append(proxy_info)
            except:
                pass
                
            
            self.queue.task_done()


# In[4]:

def minute_price_historical(symbol, comparison_symbol, limit=1, aggregate=1, exchange='', proxy = {'https' : 'https://54.39.144.247:3128'}):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url, proxies=proxy)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[5]:

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
#     print("Elapsed Time: %s" % (time.time() - start))


# In[6]:

def minute_update_one(coin, countercurrency, prox, exchange):
    client = MongoClient('localhost',
                        authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    res = []     
    lim = 1
    try:
        proxy_obj = {'https' : 'https://' + prox}
        res=minute_price_historical(coin, countercurrency, limit=lim, exchange=exchange, proxy = proxy_obj)                
        print(res[1][1])
#         
    except:
        print("No data")
        return

    if res[0]['close'].iloc[0] == 0:
                return
    cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
    data = res[1][1]
#                 print(data)
    minute_data.update({'name': exchange, 'pair': coin + '/' + countercurrency}, {'$addToSet':  {'history': data}}, upsert=True)
    minute_data.update({'name': exchange, 'pair': coin + '/' + countercurrency}, {'$set':  {'last_update': time.time()}}, upsert=True)

            


# In[7]:

def update_with_proxies(proxies):
    print(time.time())
    cntr = 0
    client = MongoClient('localhost',
                        authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    coin_pairs = db.coin_pairs
    lim = 1
    proxy_cntr = 0
    for x in range(40):
        worker = DataUpdateWorker(dataUpdateQueue)       
        worker.daemon = True
        worker.start()
        
    for data in coin_pairs.find():
        exchange = data["name"]
        pairs = data["history"]
        available = coin_pairs.find_one({'name': exchange})        
        for pair in pairs:
            tmp = pair.split('/')
            coin = tmp[0]
            countercurrency = tmp[1]
            if str(coin + '/' + countercurrency) in available["available"]:
                tmp_obj = {}                
                if proxy_cntr == len(proxies) - 1:
                    proxy_cntr = 0
                proxy = proxies[proxy_cntr]
                proxy_cntr += 1
                tmp_obj = {'coin' : coin, 'countercurrency' : countercurrency, 'proxy' : proxy, 'exchange' : exchange}
                dataUpdateQueue.put(tmp_obj)
    
    dataUpdateQueue.join()
            
    print(time.time())


# In[8]:

def main():
    # Run proxy check
    del valid_proxies[:]
    proxy_list_path = "proxies.txt"
    check_proxies(proxy_list_path, 20)
#     print(valid_proxies)
    update_with_proxies(valid_proxies)


# In[9]:

main()


# In[ ]:

f = open("proxies.txt", "r")
proxy_list = f.read().split('\n')[:-1]
for proxy_info in proxy_list:    
    proxy_handler = urllib.ProxyHandler({'http': proxy_info})
    opener = urllib.build_opener(proxy_handler)
    opener.addheaders = [('User-agent','Mozilla/5.0')]
    urllib.install_opener(opener)
    req = urllib.Request("http://www.google.com")
    try:
        sock=urllib.urlopen(req, timeout= 2)
    except:
        print('x',proxy_info)
        next
    rs = sock.read(1000)
#     print(rs)
    if '<title>Google</title>' in str(rs):
        print('0',proxy_info)
    else:
        print('x',proxy_info)
   


# In[44]:

# 13 min and 20 sec
def minute_update():   
    print(time.time())
    cntr = 0
    client = MongoClient('localhost',
                        authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    coin_pairs = db.coin_pairs
    lim = 1
    for data in coin_pairs.find():
        exchange = data["name"]
        pairs = data["history"]
#         coin_pairs.update({'name': exchange}, {'$set':  {'available': []}}, upsert=True)
        available = coin_pairs.find_one({'name': exchange})
        
            
        for pair in pairs:
            tmp = pair.split('/')
            coin = tmp[0]
            countercurrency = tmp[1]
#             print(exchange, coin, countercurrency)
            cntr += 1    
            if str(coin + '/' + countercurrency) in available["available"]:
                try:
                    res=minute_price_historical(coin, countercurrency, limit=lim, exchange=exchange)                
                    if res[0]['close'].iloc[0] == 0:
                        break
                    cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
                    data = res[1][1]
    #                 print(data)
                    minute_data.update({'name': exchange, 'pair': coin + '/' + countercurrency}, {'$addToSet':  {'history': data}}, upsert=True)
                    minute_data.update({'name': exchange, 'pair': coin + '/' + countercurrency}, {'$set':  {'last_update': time.time()}}, upsert=True)
    #                 coin_pairs.update({'name': exchange}, {'$addToSet':  {'available':  coin + '/' + countercurrency}}, upsert=True)

                except:                
                    # if no data found at all

    #                 print("      No data for currency available, Skipping")


                    break
            

        time.sleep(1)
    print(time.time())


# In[9]:

proxy_obj = {'https' : 'https://204.48.19.70:8080'}
res=minute_price_historical("BTC", "USD", limit=1, exchange="Bitfinex", proxy = proxy_obj)                
print(res[1][1])


# In[ ]:



