#!/usr/bin/env python
# -*- coding: UTF-8 -*-

'''
20190528 使用30分钟线的乖离率计算买卖信号
'''
import asyncio
import websockets
import json
import requests
import dateutil.parser as dp
import hmac
import base64
import zlib
import hashlib
import sqlite3

import websocket,time,datetime
import okex.swap_api as swap
import okex.spot_api as spot

conn = sqlite3.connect('darkCoin.db')

newPrice = dict() #最新价
candle300s = dict() #5分钟K线
#candleLst = [] #K线列表

api_key = ''
seceret_key = ''
passphrase = ''

url = 'wss://real.okex.com:10442/ws/v3'  #wss://real.okex.me:10442/ws/v3
swapAPI = swap.SwapAPI(api_key, seceret_key, passphrase, True)
spotAPI = spot.SpotAPI(api_key, seceret_key, passphrase, True)
# "spot/candle300s:EOS-USDT" 
instrumentId = "BTC-USDT"
granularity = 1800
channels = [f"spot/candle{granularity}s:{instrumentId}"]
fmtPrc = 2 # 数字格式化精度 formatPrecision


#执行Insert Delete Update
def saveLog(sql):
    print(f'sql:{sql}')
    try:
        cur =  conn.cursor()
        cur.execute(sql)
        cur.rowcount
        cur.close()
        conn.commit()
    except BaseException as ex:
        print(f" db insert error: {ex}")
    return 1

def get_server_time():
    url = "http://www.okex.com/api/general/v3/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['iso']
    else:
        return ""

def server_timestamp():
    server_time = get_server_time()
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp

def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


class Candle(object):
    def __init__(self, time, open, high, low, close, volume):
        self.time = time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
    def printC(self):
        print(f"Candle: {self.time} >> {self.open}/{self.high}/{self.low}/{self.close}/{self.volume} ")

def getAvgX(candleDic, m, n):    
    sortKeys = sorted(candleDic)
    tmpLst = [candleDic[k].close for k in sortKeys[m:n]]
    curAvg = sum(tmpLst)/len(tmpLst) #平均值
    #print(f'curAvg({m},{n}) = {curAvg}, len={len(tmpLst)})')
    return curAvg

percentGateA = 0.003
percentGateB = 0.001
upDownTag = '0' # up＝买入信号；0＝无信号；down＝卖出信号
timeTag = '' # 时间标识，同一个时间标识内只展示一次信号
timeTagPre = ''
def klineMatch():
    global upDownTag,timeTag,timeTagPre
    curAvg5 = getAvgX(candle300s, -6, -1) #平均值
    curAvg13 = getAvgX(candle300s, -14, -1) #平均值
    curAvg34 = getAvgX(candle300s, -35, -1) #平均值
    #前一批
    preAvg5 = getAvgX(candle300s, -7, -2)
    preAvg13 = getAvgX(candle300s, -15, -2)
    preAvg34 = getAvgX(candle300s, -36, -2)
    
    curTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    timeTag = candle300s[sorted(candle300s)[-1]].time

    if curAvg5 < curAvg13 < curAvg34 and preAvg5 < preAvg13 < preAvg34 and curAvg5 > preAvg5:
        #均线差值
        chaA = curAvg5 - preAvg5
        chaB = curAvg34 - curAvg13
        chaC = preAvg34 - preAvg13
        #print(f"temp: chaA={chaA}, chaB={chaB}, chaC={chaC}, chaA/curAvg5({chaA/curAvg5}), chaC/chaB({chaC/chaB})")
        if chaC > chaB and timeTagPre != timeTag:
            print(f"{curTime} up- chaC > chaB: chaA={chaA:.{fmtPrc}f}, chaB={chaB:.{fmtPrc}f}, chaC={chaC:.{fmtPrc}f} | curAvg5={curAvg5:.{fmtPrc}f}, preAvg5={preAvg5:.{fmtPrc}f}, curAvg13={curAvg13:.{fmtPrc}f}, curAvg34={curAvg34:.{fmtPrc}f}, preAvg13={preAvg13:.{fmtPrc}f}, preAvg34={preAvg34:.{fmtPrc}f}")
            upDownTag = 'up'
            timeTagPre = timeTag
            if chaA/curAvg5>percentGateA and chaC/chaB>percentGateB:
                print(f"{curTime} - up flag: chaA={chaA:.{fmtPrc}f}, chaB={chaB:.{fmtPrc}f}, chaC={chaC:.{fmtPrc}f}, chaA/curAvg5({chaA/curAvg5:.{fmtPrc}f})>{percentGateA}, chaC/chaB({chaC/chaB:.{fmtPrc}f})>{percentGateB:.{fmtPrc}f}")
    if curAvg5 > curAvg13 > curAvg34 and preAvg5 > preAvg13 > preAvg34 and curAvg5 < preAvg5:
        chaA = preAvg5 - curAvg5
        chaB = curAvg13 - curAvg34
        chaC = preAvg13 - preAvg34
        #print(f"temp: chaA={chaA}, chaB={chaB}, chaC={chaC}, chaA/curAvg5({chaA/curAvg5}), chaC/chaB({chaC/chaB})")
        if chaC > chaB and timeTagPre != timeTag:
            print(f"{curTime} -down- chaC < chaB: chaA={chaA:.{fmtPrc}f}, chaB={chaB:.{fmtPrc}f}, chaC={chaC:.{fmtPrc}f} | curAvg5={curAvg5:.{fmtPrc}f}, preAvg5={preAvg5:.{fmtPrc}f}, curAvg13={curAvg13:.{fmtPrc}f}, curAvg34={curAvg34:.{fmtPrc}f}, preAvg13={preAvg13:.{fmtPrc}f}, preAvg34={preAvg34:.{fmtPrc}f}")
            upDownTag = 'down'
            timeTagPre = timeTag
            if chaA/curAvg5>percentGateA and chaC/chaB>percentGateB:
                print(f"{curTime} --> down flag: chaA={chaA:.{fmtPrc}f}, chaB={chaB:.{fmtPrc}f}, chaC={chaC:.{fmtPrc}f}, chaA/curAvg5({chaA/curAvg5:.{fmtPrc}f})>{percentGateA}, chaC/chaB({chaC/chaB:.{fmtPrc}f})>{percentGateB}")


def on_message(ws, message):
    # print('on_message.ws:', ws)
    #print("receive:")
    res = inflate(message)
    # print(f"receive:{res}")

    resDict = json.loads(res)
    #print(f"resDict: {resDict}")
    if 'event' in resDict:
        evt = resDict['event']
        if evt == 'login' and (resDict['success'] == 'true' or resDict['success'] == True):
            #请求账户数据
            # sub_param = {"op": "subscribe", "args": channels}
            # sub_str = json.dumps(sub_param)
            # ws.send(sub_str)
            #print(f"login succesfull subscribe...send: {sub_str}")
            pass
        elif evt == 'error':
            logstr = f"sth err, errorCode: {resDict['errorCode']}, message: {resDict['message']}"
            print(logstr)
            saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'error','{logstr}')")
        elif evt == 'subscribe':
            print(f"subscribe succesfull, channel: {resDict['channel']}")
        else:
            print(f"unknow event: {resDict['event']}")
    elif 'table' in resDict:
        resTbl = resDict['table']
        resData = resDict['data'] if 'data' in resDict else {}
        if resTbl == f'spot/candle{granularity}s': #最新价
            #{"table": "spot/candle300s","data": 
            #"candle":["2019-04-16T10:49:00.000Z","open","high","low","close","volume"]
            #更新K线列表
            for curCandle in resData:
                tc = curCandle['candle']
                aaT = time.strptime(tc[0][0:-1],'%Y-%m-%dT%H:%M:%S.%f')
                timeVal = time.strftime("%Y%m%d%H%M", aaT)
                #(f'time: {tc[0]} ---- {aaT} ---- {time.strftime("%Y%m%d%H%M%S", aaT)}')
                newCandle = Candle(timeVal,float(tc[1]),float(tc[2]),float(tc[3]),float(tc[4]),float(tc[5]))
                candle300s[newCandle.time] = newCandle
                timeTag = newCandle.time
                #newCandle.printC()
            # 判断交易标志
            klineMatch()
    else:
        print(f'other result: {resDict}')

def on_error(ws, error):
    # print(ws)
    print(error)

def on_close(ws):
    # print(ws)
    print("### closed ###")
    #time.sleep(3)
    #print("### reconnect... ###")
    #theManager(url)
    #asyncio.get_event_loop().run_until_complete(theManager(url))

def on_open(ws):

    # login
    timestamp = server_timestamp()
    print(f"server_timestamp:{timestamp}")
    # login_str = login_params(str(timestamp), api_key, passphrase, seceret_key)
    # print(f"login: {login_str}")
    # ws.send(login_str)
    # saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'login','user login')")
    
    # 首次获取前n条kline
    aaT = time.gmtime(timestamp - granularity*40)
    beginT = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", aaT)
    endT = get_server_time()
    # 时间粒度granularity必须是[60 180 300 900 1800 3600 7200 14400 21600 43200 86400 604800]中的任一值
    # 分别对应的是[1min 3min 5min 15min 30min 1hour 2hour 4hour 6hour 12hour 1day 1week]的时间段
    # klines = swapAPI.get_kline(instrumentId, 1800, beginT, endT)
    klines = spotAPI.get_kline(instrumentId, beginT, endT, granularity)
    # print(f"kline time: {beginT} -- {endT} >>> {klines}")
    #更新K线列表
    for tc in klines:
        #tc = curCandle['candle']
        aaT = time.strptime(tc[0][0:-1],'%Y-%m-%dT%H:%M:%S.%f')
        timeVal = time.strftime("%Y%m%d%H%M", aaT)
        (f'time: {tc[0]} ---- {aaT} ---- {time.strftime("%Y%m%d%H%M%S", aaT)}')
        newCandle = Candle(timeVal,float(tc[1]),float(tc[2]),float(tc[3]),float(tc[4]),float(tc[5]))
        candle300s[newCandle.time] = newCandle
        newCandle.printC()
    print(f"kline time: {beginT} -- {endT} >>> klines.count:{len(candle300s)}")
    
    #订阅ws的K线
    # channels = ["spot/ticker:BTC-USDT"]
    sub_param = {"op": "subscribe", "args": channels}
    sub_str = json.dumps(sub_param)
    ws.send(sub_str)


async def theManager(url):
    try:
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(url,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.on_open = on_open
        ws.run_forever(ping_interval=25, ping_timeout=20)
        print('........chinese.输出中文...'.encode('utf8'))
    except Exception as ex:
        print(f"*-*-*-*-*-*-*-* app error:{ex}")
    finally:
        print('close conn and websocket...')
        if ws and ws is not None:
            ws.close()
        if conn and conn is not None:
            conn.close()


# asyncio.get_event_loop().run_until_complete(login(url, api_key, passphrase, seceret_key))

# asyncio.get_event_loop().run_until_complete(subscribe(url, api_key, passphrase, seceret_key, channels))
# asyncio.get_event_loop().run_until_complete(unsubscribe(url, api_key, passphrase, seceret_key, channels))
# asyncio.get_event_loop().run_until_complete(subscribe_without_login(url, channels))
# asyncio.get_event_loop().run_until_complete(unsubscribe_without_login(url, channels))
asyncio.get_event_loop().run_until_complete(theManager(url))