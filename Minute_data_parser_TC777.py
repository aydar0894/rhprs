
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
dataUpdateQueue = Queue()
WsQueue = Queue()

result_minutes = {}


# In[2]:

class WsWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            ws = self.queue.get()
            ws.run_forever()
            self.queue.task_done()


# In[3]:

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


#             print(info)
            minute_update_one(coin, countercurrency, prox, exchange)
            self.queue.task_done()


# In[4]:

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


# In[5]:

def daily_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange='', proxy = {'https' : 'https://54.39.144.247:3128'}):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&allData=true'    .format(symbol.upper(), comparison_symbol.upper())
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url, proxies=proxy, timeout=2)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[6]:

def hourly_price_historical(symbol, comparison_symbol, toTs, limit=100, aggregate=1, exchange='', proxy = {'https' : 'https://54.39.144.247:3128'}):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym={}&limit={}&aggregate={}&toTs={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate, toTs)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url, proxies=proxy, timeout=2)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[7]:

def minute_price_historical(symbol, comparison_symbol, limit=1, aggregate=1, exchange='', proxy = {'https' : 'https://54.39.144.247:3128'}):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}'    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url, proxies=proxy, timeout=2)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return [df, data]


# In[8]:

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
    f.close()
    print(len(valid_proxies))
#     print("Elapsed Time: %s" % (time.time() - start))


# In[9]:

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
        if res[0]['close'].iloc[0] == 0:
                    return
        cols = ['timestamp', 'time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
        data = res[1][1].copy()
        current_info = minute_data.find_one({'name': exchange, 'pair': coin + '/' + countercurrency})
        tmp_all_data = current_info["history"]
        tmp_24 = tmp_all_data[-60*24:]
        tmp_7d = tmp_all_data[-60*24*7:]
        tmp_30d = tmp_all_data[-60*24*30:]
        change_24 = tmp_24[len(tmp_24) - 1]['close'] - tmp_24[0]['close']
        change_7d = tmp_7d[len(tmp_7d) - 1]['close'] - tmp_7d[0]['close']
        change_30d = tmp_30d[len(tmp_30d) - 1]['close'] - tmp_30d[0]['close']
        data['change_24'] = change_24
        data['change_7d'] = change_7d
        data['change_30d'] = change_30d
        result_minutes.update({exchange + '_' + coin + '/' + countercurrency : str(data)})
    #                 print(data)
        minute_data.update({'name': exchange, 'pair': coin + '/' + countercurrency}, {'$addToSet':  {'history': res[1][1]}}, upsert=True)
        minute_data.update({'name': exchange, 'pair': coin + '/' + countercurrency}, {'$set':  {'last_update': time.time(), 'change_24' : change_24, 'change_7d' : change_7d, 'change_30d' : change_30d}}, upsert=True)


#         print(res[1][1])
#
    except:
#         print("No data")
        return




# In[10]:

def hourly_update_one(coin, countercurrency, prox, exchange):
    client = MongoClient('localhost',
                        authSource='bitcoin')
    db = client.bitcoin
    hourly_data = db.hourly_data
    res = []
    lim = 1
    try:
        proxy_obj = {'https' : 'https://' + prox}
        res=hourly_price_historical(coin, countercurrency, limit=lim, exchange=exchange, proxy = proxy_obj)
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
    hourly_data.update({'name': exchange, 'pair': coin + '/' + countercurrency}, {'$addToSet':  {'history': data}}, upsert=True)
    hourly_data.update({'name': exchange, 'pair': coin + '/' + countercurrency}, {'$set':  {'last_update': time.time()}}, upsert=True)


# In[11]:

arr = [1,2,3,4,5,6,7,8,9,10]
print(arr[-(3*2):])


# In[12]:

def minute_update_with_proxies(proxies, ws):
    time_start = time.time()
    cntr = 0
    client = MongoClient('localhost',
                        authSource='bitcoin')
    db = client.bitcoin
    minute_data = db.minute_data
    coin_pairs = db.coin_pairs
    lim = 1
    proxy_cntr = 0

    try:
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
    except:
        return

    for x in range(40):
        worker = DataUpdateWorker(dataUpdateQueue)
        worker.daemon = True
        worker.start()

    dataUpdateQueue.join()
    ws.send(str(result_minutes))
    print(result_minutes)
    result_minutes.clear()
    print(time.time() - time_start)


# In[13]:

def on_message(ws, message):
    print("Message")
def on_open(ws):
    ws.send("market_screener_updater")
    print("Opened")
def on_error(ws, error):
    print(error)
def on_close(ws):
    print("Closed")


# In[14]:

def start(ws):
    # Run proxy check
    del valid_proxies[:]
    proxy_list_path = "proxies.txt"
    check_proxies(proxy_list_path, 20)

#     print(valid_proxies)
    minute_update_with_proxies(valid_proxies, ws)


# In[15]:

def main():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://localhost:8800/marketsc",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    WsQueue.put(ws)
    worker = WsWorker(WsQueue)
    worker.daemon = True
    worker.start()

#     ws.run_forever()

    while True:
        prev_time = time.time()
        start(ws)
        if time.time() - prev_time < 60:
            time.sleep(60-(time.time() - prev_time))


# In[ ]:

main()



# In[44]:

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



# In[ ]:

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


# In[ ]:

proxy_obj = {'https' : 'https://204.48.19.70:8080'}
res=minute_price_historical("BTC", "USD", limit=1, exchange="Bitfinex", proxy = proxy_obj)
print(res[1][1])


# In[47]:

result = {}
result.update({"Bitfinex_btcds/usd" : 4})

print(result)


# In[ ]:
