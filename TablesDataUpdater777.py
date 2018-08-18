import datetime
import pandas as pd
import time
import matplotlib.pyplot as plt
import time
from pymongo import MongoClient
import datetime
import sys
import json
import os
from xml.etree import ElementTree
import re
from queue import Queue
from threading import Thread
import websocket


# In[ ]:

def on_message(ws, message):
    client = MongoClient('localhost',
                        authSource='bitcoin'
                        )
    db = client.bitcoin
    exchanges_col = db.exchanges
    pools_col = db.pools
    whale_wallets_col = db.whale_wallets
    wallets_col = db.wallets

    newm = message.replace('\'', '\"')
    d = json.loads(str(newm))
    pools = {}
    exchanges = {}
    whaleWallets = {}
    updatedWallets = {}


    try:
        pools = d["pools"]
    except:
        next
    try:
        exchanges = d["exchanges"]
    except:
        next
    try:
        whaleWallets = d["whaleWallets"]
    except:
        next
    try:
        updatedWallets = d["updatedWallets"]
    except:
        next

#     Remove old changes

    pools_col.update_many({},
                          {
                          '$pull': {
                            'changes_24': {
                                'timestamp': {'$lte': time.time() - 24*60*60}
                            }
                          }
                        }, upsert=False)


    exchanges_col.update_many({},
                          {
                          '$pull': {
                            'changes_24': {
                                'timestamp': {'$lte': time.time() - 24*60*60}
                            }
                          }
                        }, upsert=False)
    whale_wallets_col.update_many({},
                              {
                              '$pull': {
                                'changes_24': {
                                    'timestamp': {'$lte': time.time() - 24*60*60}
                                }
                              }
                            }, upsert=False)
    wallets_col.update_many({},
                          {
                          '$pull': {
                            'changes_24': {
                                'timestamp': {'$lte': time.time() - 24*60*60}
                            }
                          }
                        }, upsert=False)




    for pool in pools:

        pool_info = pools_col.find_one({"name": pools[pool]['name']})
        try:
            prev_change = sum([pair['change'] for pair in pool_info['changes_24']])
        except:
            prev_change = 0
        try:
            prev_turnover = sum([abs(pair['change']) for pair in pool_info['changes_24']])
        except:
            prev_turnover = 0

        try:
            av_change = pool_info['av_change']
        except:
            av_change = 0
        try:
            num_changes = pool_info['num_changes']
        except:
            num_changes = 0
        try:
            av_turnover = pool_info['av_turnover']
        except:
            av_turnover = 0
        try:
            num_turnovers = pool_info['num_turnovers']
        except:
            num_turnovers = 0

        change_24 = (prev_change + int(pools[pool]['amount']))/100000000
        turnover_24 = (prev_turnover + abs(float(pools[pool]['amount'])))/100000000

        new_av_change = (float(av_change) * int(num_changes) + change_24)/(int(num_changes)+1)
        new_av_turnover = (float(av_turnover) * int(num_turnovers) + abs(turnover_24))/(int(num_turnovers)+1)

        pools[pool]["change_24"] = change_24
        pools[pool]["av_change"] = new_av_change
        pools[pool]["av_turnover"] = new_av_turnover
        pools[pool]["turnover_24"] = turnover_24

        pools_col.update_one({
                              'name': pools[pool]['name']
                            },{
                              '$push': {
                                'changes_24': {
                                    'timestamp': time.time(),
                                    'change': pools[pool]['amount']
                                }
                              }
                            }, upsert=True)
        pools_col.update_one({
                              'name': pools[pool]['name']
                            },{
                              '$set': {
                                'change_24' : change_24,
                                'turnover_24' : turnover_24,
                                'av_change' : new_av_change,
                                'num_changes' : int(num_changes)+1,
                                'av_turnover' : new_av_turnover,
                                'num_turnovers' : int(num_turnovers)+1
                              }
                            }, upsert=True)

    for exchange in exchanges:

        exchange_info = exchanges_col.find_one({"name": exchanges[exchange]['name']})
        try:
            prev_change = sum([pair['change'] for pair in exchange_info['changes_24']])
        except:
            prev_change = 0
        try:
            prev_turnover = sum([abs(pair['change']) for pair in exchange_info['changes_24']])
        except:
            prev_turnover = 0

        try:
            av_change = exchange_info['av_change']
        except:
            av_change = 0
        try:
            num_changes = exchange_info['num_changes']
        except:
            num_changes = 0
        try:
            av_turnover = exchange_info['av_turnover']
        except:
            av_turnover = 0
        try:
            num_turnovers = exchange_info['num_turnovers']
        except:
            num_turnovers = 0

        change_24 = (prev_change + int(exchanges[exchange]['amount']))/100000000
        turnover_24 = (prev_turnover + abs(float(exchanges[exchange]['amount'])))/100000000

        new_av_change = (float(av_change) * int(num_changes) + change_24)/(int(num_changes)+1)
        new_av_turnover = (float(av_turnover) * int(num_turnovers) + abs(turnover_24))/(int(num_turnovers)+1)


        exchanges[exchange]["change_24"] = change_24
        exchanges[exchange]["turnover_24"] = turnover_24
        exchanges[exchange]["av_change"] = new_av_change
        exchanges[exchange]["av_turnover"] = new_av_turnover

        exchanges_col.update_one({
                              'name': exchanges[exchange]['name']
                            },{
                              '$push': {
                                'changes_24': {
                                    'timestamp': time.time(),
                                    'change': exchanges[exchange]['amount']
                                }
                              }
                            }, upsert=True)
        exchanges_col.update_one({
                              'name': exchanges[exchange]['name']
                            },{
                              '$set': {
                                'change_24' : change_24,
                                'turnover_24' : turnover_24,
                                'av_change' : new_av_change,
                                'num_changes' : int(num_changes)+1,
                                'av_turnover' : new_av_turnover,
                                'num_turnovers' : int(num_turnovers)+1
                              }
                            }, upsert=True)

    for whaleWallet in whaleWallets:

        whale_wallet_info = whale_wallets_col.find_one({"address": whaleWallets[whaleWallet]['address']})
        try:
             prev_change = sum([pair['change'] for pair in whale_wallet_info['changes_24']])
        except:
            prev_change = 0
        try:
            prev_turnover = sum([abs(pair['change']) for pair in whale_wallet_info['changes_24']])
        except:
            prev_turnover = 0

        try:
            av_change = whale_wallet_info['av_change']
        except:
            av_change = 0
        try:
            num_changes = whale_wallet_info['num_changes']
        except:
            num_changes = 0
        try:
            av_turnover = whale_wallet_info['av_turnover']
        except:
            av_turnover = 0
        try:
            num_turnovers = whale_wallet_info['num_turnovers']
        except:
            num_turnovers = 0

        change_24 = (prev_change + int(whaleWallets[whaleWallet]['amount']))/100000000
        turnover_24 = (prev_turnover + abs(whaleWallets[whaleWallet]['amount']))/100000000

        new_av_change = (float(av_change) * int(num_changes) + change_24)/(int(num_changes)+1)
        new_av_turnover = (float(av_turnover) * int(num_turnovers) + abs(turnover_24))/(int(num_turnovers)+1)




        whaleWallets[whaleWallet]["change_24"] = change_24
        whaleWallets[whaleWallet]["turnover_24"] = turnover_24
        whaleWallets[whaleWallet]["av_change"] = new_av_change
        whaleWallets[whaleWallet]["av_turnover"] = new_av_turnover

        whale_wallets_col.update_one({
                              'address': whaleWallets[whaleWallet]['address']
                            },{
                              '$push': {
                                'changes_24': {
                                    'timestamp': time.time(),
                                    'change': whaleWallets[whaleWallet]['amount']
                                }
                              }
                            }, upsert=True)
        whale_wallets_col.update_one({
                              'address': whaleWallets[whaleWallet]['address']
                            },{
                              '$set': {
                                'change_24' : change_24,
                                'turnover_24' : turnover_24,
                                'av_change' : new_av_change,
                                'num_changes' : int(num_changes)+1,
                                'av_turnover' : new_av_turnover,
                                'num_turnovers' : int(num_turnovers)+1
                              }
                            }, upsert=True)

    for updatedWallet in updatedWallets:

        updated_wallet_info = wallets_col.find_one({"address": updatedWallets[updatedWallet]['address']})
        try:
            prev_change = sum([pair['change'] for pair in updated_wallet_info['changes_24']])
        except:
            prev_change = 0
        try:
            prev_turnover = sum([abs(pair['change']) for pair in updated_wallet_info['changes_24']])
        except:
            prev_turnover = 0

        try:
            av_change = updated_wallet_info['av_change']
        except:
            av_change = 0
        try:
            num_changes = updated_wallet_info['num_changes']
        except:
            num_changes = 0
        try:
            av_turnover = updated_wallet_info['av_turnover']
        except:
            av_turnover = 0
        try:
            num_turnovers = updated_wallet_info['num_turnovers']
        except:
            num_turnovers = 0

        change_24 = (prev_change + int(updatedWallets[updatedWallet]['amount']))/100000000
        turnover_24 = (prev_turnover + abs(updatedWallets[updatedWallet]['amount']))/100000000

        new_av_change = (float(av_change) * int(num_changes) + change_24)/(int(num_changes)+1)
        new_av_turnover = (float(av_turnover) * int(num_turnovers) + abs(turnover_24))/(int(num_turnovers)+1)





        updatedWallets[updatedWallet]["change_24"] = change_24
        updatedWallets[updatedWallet]["av_change"] = change_24
        updatedWallets[updatedWallet]["av_turnover"] = new_av_change
        updatedWallets[updatedWallet]["turnover_24"] = new_av_turnover

        wallets_col.update_one({
                              'address': updatedWallets[updatedWallet]['address']
                            },{
                              '$push': {
                                'changes_24': {
                                    'timestamp': time.time(),
                                    'change': updatedWallets[updatedWallet]['amount']
                                }
                              }
                            }, upsert=True)
        wallets_col.update_one({
                              'address': updatedWallets[updatedWallet]['address']
                            },{
                              '$set': {
                                'change_24' : change_24,
                                'turnover_24' : turnover_24,
                                'av_change' : new_av_change,
                                'num_changes' : int(num_changes)+1,
                                'av_turnover' : new_av_turnover,
                                'num_turnovers' : int(num_turnovers)+1
                              }
                            }, upsert=True)




    result = {"pools": pools, "exchanges": exchanges, "whaleWallets": whaleWallets, "updatedWallets": updatedWallets}
    print(result)
    ws.send(str(result))

def on_open(ws):
    ws.send("python ws")
    print("Opened")
def on_error(ws, error):
    print(error)
def on_close(ws):
    print("Closed")


# In[ ]:

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://localhost:8800/tables",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
