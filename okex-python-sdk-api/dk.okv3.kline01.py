#!/usr/bin/env python
'''
201905，收盘价超过3均线0.1%时开单或者平仓
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

pars = {'buyOrSell': 'buy'}
newPrice = dict() #最新价
myAccount = dict() #结构：{'instrument_id':{swap/account}}
myPositionA = dict() #多仓
myPositionB = dict() #空仓
upOrderPair = dict() #多单对，当前价上下各浮动n%同时开多和平多，一边成交则撤单另一个 {'自定ID':'id','a单':{},'b单':{}}
downOrderPair = dict() #空单对
buyPctGate = 0.002 #开仓阈值
marginRatioGate =  0.25 #保证金率阈值，低于此值停止开单| 保证金率=（账户余额+已实现盈亏+未实现盈亏）／（面值*张数／最新标记价格+冻结保证金*杠杆倍数）
amount = 1 #开仓张数
sleepGate = 0.06 #下单延时阈值
positionGuardGate = 0.1 #持仓差异守护：为防止多空单持仓的差异过大，增加守护逻辑，超过阈值时停止数量多一边的加仓，量少的一边不停。
candle300s = dict() #5分钟K线
klineFlagGate = 0.001 #K线交易阈值

api_key = ''
seceret_key = ''
passphrase = ''
url = 'wss://real.okex.com:10442/ws/v3'
swapAPI = swap.SwapAPI(api_key, seceret_key, passphrase, True)
spotAPI = spot.SpotAPI(api_key, seceret_key, passphrase, True)
# "spot/candle300s:EOS-USDT" 
channels = ["swap/account:EOS-USD-SWAP", "swap/position:EOS-USD-SWAP", "swap/order:EOS-USD-SWAP", "swap/candle300s:EOS-USD-SWAP"]
instrumentId = "EOS-USD-SWAP"

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

def login_params(timestamp, api_key, passphrase, secret_key):
    message = timestamp + 'GET' + '/users/self/verify'
    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {"op": "login", "args": [api_key, passphrase, timestamp, sign.decode("utf-8")]}
    login_str = json.dumps(login_param)
    return login_str

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



# 将订单保存到本地单对
def refreshOrderPair(order, orderPair):
    clientOid = order['client_oid']
    orderType = order['type']
    if clientOid in orderPair:
        if orderType == '1' or orderType == '2':
            orderPair[clientOid]['a'] = order
        else:
            orderPair[clientOid]['b'] = order
    else:
        tA = order if orderType == '1' or orderType == '2' else None
        tB = order if orderType == '3' or orderType == '4' else None
        orderPair[clientOid] = {'a': tA, 'b': tB}
    print(f"update orderPair.clientOid: {clientOid}, orderType:{orderType}")


# 撤单
def revokeOrder(curOrder, myOrderPair):
    clientId = curOrder['client_oid'] if 'client_oid' in curOrder else None
    if clientId is not None:
        curPair = myOrderPair[clientId] if clientId in myOrderPair else None
        if curPair is not None:
            #print(f'------------ curPair: {curPair}')
            orderA = curPair['a']
            orderB = curPair['b']
            revokeOid = None
            #撤单
            if orderA is not None and orderA['order_id'] != curOrder['order_id']:                                    
                revokeOid = orderA['order_id']
            elif orderB is not None and orderB['order_id'] != curOrder['order_id']:
                revokeOid = orderB['order_id']
            if revokeOid is not None and revokeOid != '-1':
                result2 = swapAPI.revoke_order(revokeOid, 'EOS-USD-SWAP')
                print(f'revoke_order[{revokeOid}].result: {result2}')
            else:
                print(f'no order to revoke... revokeOid = {revokeOid}')
        else:
            print('curPair is none...')
    else:
        print('clientId is none ...')
    # save order
    # saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'swap/ticker:EOS-USD-SWAP','{res.decode()}')")
    saveLog(f'''insert into orders values(null,'{curOrder['instrument_id']}','{curOrder['order_id']}','{curOrder['client_oid']}','{curOrder['timestamp']}'
        ,{curOrder['state']},{curOrder['size']},{curOrder['filled_qty']},{curOrder['price']},{curOrder['price_avg']},{curOrder['type']}
        ,{curOrder['order_type']},{curOrder['fee']},{curOrder['last_fill_px']},{curOrder['last_fill_qty']},'{curOrder['last_fill_time']}'
        ,{curOrder['contract_val']})''')

# 开单 inPrice=开单价；outPrice=平单价
def takeOrder(instrumentId, myPstn, curPrice, inPrice, outPrice, typeUp, typeDown, actionType):
    if myAccount is None or instrumentId not in myAccount:
        print(f"")
        return 0        
    marginRatio = float(myAccount[instrumentId]['margin_ratio'], 1) #保证金率
    equity = float(myAccount[instrumentId]['equity']) #账户权益
    longMargin = myPositionA[instrumentId]['margin'] if instrumentId in myPositionA else 0
    shortMargin = myPositionB[instrumentId]['margin'] if instrumentId in myPositionB else 0
    positionGuard = (float(longMargin) - float(shortMargin)) / equity
    if positionGuard > positionGuardGate:
        positionGuardType = 1
    elif positionGuard < -positionGuardGate:
        positionGuardType = -1
    else:
        positionGuardType = 0
    canOrderUp = actionType == 'up' and positionGuardType != 1
    canOrderDown = actionType == 'down' and positionGuardType != -1
    clientOid = 'dk' + time.strftime('%Y%m%d%H%M%S')

    if marginRatio > marginRatioGate and (canOrderUp or canOrderDown):
        time.sleep(sleepGate)
        #client_oid 是数字+字母（大小写）或者纯字母（大小写）类型 1-32位
        logstr = f'take_orders {typeUp} clientOid: {clientOid}, curPrice: {curPrice}, inPrice: {inPrice}, outPrice: {outPrice}'
        print(logstr)
        result = swapAPI.take_orders([
            {"client_oid": clientOid,"price": str(inPrice),"size": amount,"type": typeUp,"match_price": "0"},
            {"client_oid": clientOid,"price": str(outPrice),"size": amount,"type": typeDown,"match_price": "0"}
        ],instrumentId)
        print(f'take_orders.{typeUp}.result: {result}')
        saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'swap/take_orders','{logstr}')")
        for r in result['order_info']:
            if r['error_code'] != '0':
                logstr = f"take_orders.{typeUp}.error: code={r['error_code']}, error_message: {r['error_message']}, clientOid: {clientOid}"
                print(logstr)
                saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'swap/take_orders.error','{logstr}')")
            else:
                logstr = f"take_orders.{typeUp}.order: {r['client_oid']}<->{r['order_id']}"
                print(logstr)
                saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'swap/take_orders.result','{logstr}')")
    elif instrumentId in myPstn:
        #保证金低，只平
        curPosition = myPstn[instrumentId]['avail_position']
        if int(curPosition) > amount:
            print(f"stop add order, margin ratio is low: {marginRatio}")
            logstr = f"take_order {typeUp}: ({instrumentId},{amount},{typeDown},{str(outPrice)},{clientOid},'')"
            print(logstr)
            result = swapAPI.take_order(instrumentId,amount,typeDown,str(outPrice),clientOid,'')
            print('3-----', result)
            if result['result'] == 'true':
                print(f"take_order.{typeUp}.order: {result['client_oid']}<->{result['order_id']}")
                saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'swap/take_order','{logstr}')")
            else:
                print(f"take_order.error:{result['error_message']} >>>{typeUp}.order: {result['client_oid']}<->{result['order_id']}")
        else:
            print(f'{typeUp} position is low: {curPosition}')
    else:
        print(f'take order not action: {instrumentId}, clientOid={clientOid}')
        print(f'marginRatio:{marginRatio}, canOrderUp: {canOrderUp}, canOrderDown: {canOrderDown}, myPstn:{myPstn}')

# 市价开单
def takeOrderMatch(instrumentId):
    buyOrSell = pars['buyOrSell']
    print('...takeOrderMatch....', buyOrSell)
    if myAccount is None or instrumentId not in myAccount:
        print(f"cant take order, myAccount data is empty: {myAccount}")
        return 0        
    clientOid = 'dk' + time.strftime('%Y%m%d%H%M%S')
    if buyOrSell == 'buy':
        # 开单
        mr = myAccount[instrumentId]['margin_ratio']
        marginRatio = float(mr) if mr is not None and mr != '' else 1 #保证金率
        if marginRatio > marginRatioGate:
            #client_oid 是数字+字母（大小写）或者纯字母（大小写）类型 1-32位
            logstr = f"take_order {buyOrSell}: ({instrumentId},{amount},{clientOid},'')"
            print(logstr)
            result = swapAPI.take_order(instrumentId,amount,'1','',clientOid,'1')
            print('swapAPI.take_order.result-----', result)
            if result['result'] == 'true':
                buyOrSell = 'sell'
                print(f"take_order.{buyOrSell}.order: {result['client_oid']}<->{result['order_id']} current flag: {buyOrSell}")
                saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'swap/take_order','{logstr}')")
            else:
                print(f"take_order.error:{result['error_message']} >>>{buyOrSell}.order: {result['client_oid']}<->{result['order_id']}")
        else:
            print(f"stop add order, margin ratio is low: {marginRatio}")
    else:
        if instrumentId not in myPositionA:
            pars['buyOrSell'] = 'buy'
            print(f'cant take order, myPositionA is empty:, {myPositionA} flag turn to {buyOrSell}')
            return 0
        # 平仓
        curPosition = myPositionA[instrumentId]['avail_position']
        if int(curPosition) >= amount:
            logstr = f"take_order {buyOrSell}: ({instrumentId},{amount},{clientOid},'')"
            print(logstr)
            result = swapAPI.take_order(instrumentId,amount,'3','',clientOid,'1')
            print('swapAPI.take_order.result-----', result)
            if result['result'] == 'true':
                buyOrSell = 'buy'
                print(f"take_order.{buyOrSell}.order: {result['client_oid']}<->{result['order_id']} current flag: {buyOrSell}")
                saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'swap/take_order','{logstr}')")
            else:
                print(f"take_order.error:{result['error_message']} >>>{buyOrSell}.order: {result['client_oid']}<->{result['order_id']}")
        else:
            buyOrSell = 'buy'
            print(f'take order not action, position is low: {curPosition}, current flag turn to {buyOrSell}')
    pars['buyOrSell'] = buyOrSell
    print(f"pars['buyOrSell'] = {pars['buyOrSell']}")

def klineMatch():
    buyOrSell = pars['buyOrSell']
    #print(f'---------------klineMatch--------------------------------------------------- {buyOrSell}')
    sortKeys = sorted(candle300s)
    #print('sortKeys:', type(sortKeys), sortKeys)
    lastCandle = candle300s[sortKeys[-1]] #取最后一个K线
    #print('lastCandle', lastCandle.__dict__)

    closeLst = [candle300s[k].close for k in sortKeys[-3:]]
    curAvg = sum(closeLst)/len(closeLst) #平均值
    #print('close prices: ', closeLst, curAvg)

    # L = [v.close for k,v in candle300s.items()]
    # print(L)

    lastClose = lastCandle.close #最新收盘价
    if buyOrSell == 'buy' and lastClose/curAvg > (1+klineFlagGate):
        #买入
        print(f'will buy, lastClose:{lastClose}, curAverage:{curAvg}, lastClose/curAvg={lastClose/curAvg}')
        takeOrderMatch(instrumentId)
    elif buyOrSell == 'sell' and lastClose/curAvg < (1-klineFlagGate):
        #卖出
        print(f'will sell, lastClose:{lastClose}, curAverage:{curAvg}, lastClose/curAvg={lastClose/curAvg}')    
        takeOrderMatch(instrumentId)
    else:
        print(f'buyOrSell: {buyOrSell}, lastClose:{lastClose}, curAverage:{curAvg}, lastClose/curAvg={lastClose/curAvg}  --- {buyOrSell}')


def on_message(ws, message):
    # print('on_message.ws:', ws)
    #print("receive:")
    res = inflate(message)
    #print(f"receive:{res}")

    resDict = json.loads(res)
    #print(f"resDict: {resDict}")
    if 'event' in resDict:
        evt = resDict['event']
        if evt == 'login' and (resDict['success'] == 'true' or resDict['success'] == True):
            #请求账户数据
            sub_param = {"op": "subscribe", "args": channels}
            sub_str = json.dumps(sub_param)
            ws.send(sub_str)
            #print(f"login succesfull subscribe...send: {sub_str}")
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
        if resTbl == 'swap/candle300s': #最新价
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
                #newCandle.printC()
            # 判断交易标志
            klineMatch()
        if resTbl == 'swap/account': #账户状态
            #(f"account: {resData}")
            #更新账户状态
            for curOne in resData:
                myAccount[curOne['instrument_id']] = curOne
                print(f"new myAccount, equity: {curOne['equity']}, margin_ratio: {curOne['margin_ratio']}")
        if resTbl == 'swap/position': #持仓
            #print(f"position: {resData}")
            for curOne in resData:
                for holdT in curOne['holding']:
                    if holdT['side'] == 'long':
                        myPositionA[curOne['instrument_id']] = holdT
                        print(f"new long.position: {holdT['position']}")
                    else:
                        myPositionB[curOne['instrument_id']] = holdT
                        print(f"new short.position: {holdT['position']}")
        if resTbl == 'swap/order': #
            return 1 #K线模式不处理订单
            #state 订单状态("-2":失败,"-1":撤单成功,"0":等待成交 ,"1":部分成交, "2":完全成交,"3":下单中,"4":撤单中,）
            #type 1:开多 2:开空 3:平多 4:平空
            #print(f"order: {resData}")
            for order in resData:
                orderPrice = float(order['price'])
                curPrice = float(newPrice['last']) if 'last' in newPrice else orderPrice
                if order['type'] == '1' or order['type'] == '3':
                    curPair = upOrderPair
                    outPrice = curPrice * (1+buyPctGate)
                    inPrice = curPrice * (1-buyPctGate)
                    thisPosition = myPositionA
                    odrTypeA = '1'
                    odrTypeB = '3'
                    actionType = 'up'
                elif order['type'] == '2' or order['type'] == '4':
                    curPair = downOrderPair
                    inPrice = curPrice * (1+buyPctGate)
                    outPrice = curPrice * (1-buyPctGate)
                    thisPosition = myPositionB
                    odrTypeA = '2'
                    odrTypeB = '4'
                    actionType = 'down'
                else:
                    print(f"unhandle order.type: {order['type']}, order: {order}")
                
                if order['state'] == '0':
                    #下单成功，保存本地
                    refreshOrderPair(order, curPair)
                elif order['state'] == '2':
                    # 挂单成交，撤旧单，挂新单对
                    # 撤单
                    revokeOrder(order, curPair)
                    print('撤单...')
                    # 开新单
                    takeOrder(order['instrument_id'], thisPosition, curPrice, inPrice, outPrice, odrTypeA, odrTypeB, actionType)
                    print('开新单...')
                elif order['state'] == '-1':
                    #撤单成功
                    print(f"revoke order successfull clientOid:{order['client_oid']}, orderId:{order['order_id']}")
                    #删除记录对
                    if 'client_oid' in order and order['client_oid'] in curPair:
                        curPair.pop(order['client_oid'])
                else:
                    print(f"unhandle order state: {order['state']}")
    else:
        print(f'unknow result: {resDict}')

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
    #channels = ["swap/ticker:BTC-USD-SWAP"]
    #sub_param = {"op": "subscribe", "args": channels}
    #sub_str = json.dumps(sub_param)

    # login
    timestamp = server_timestamp()
    login_str = login_params(str(timestamp), api_key, passphrase, seceret_key)
    print(f"login: {login_str}")
    ws.send(login_str)
    # saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'login','user login')")
    
    # 首次获取前200条kline
    aaT = time.gmtime(timestamp-7200)
    beginT = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", aaT)
    endT = get_server_time()
    # 时间粒度granularity必须是[60 180 300 900 1800 3600 7200 14400 21600 43200 86400 604800]中的任一值
    # 分别对应的是[1min 3min 5min 15min 30min 1hour 2hour 4hour 6hour 12hour 1day 1week]的时间段
    klines = swapAPI.get_kline('EOS-USD-SWAP', 300, beginT, endT)
    #print(f"kline time: {beginT} -- {endT} >>> {klines}")
    #更新K线列表
    for tc in klines:
        #tc = curCandle['candle']
        aaT = time.strptime(tc[0][0:-1],'%Y-%m-%dT%H:%M:%S.%f')
        timeVal = time.strftime("%Y%m%d%H%M", aaT)
        (f'time: {tc[0]} ---- {aaT} ---- {time.strftime("%Y%m%d%H%M%S", aaT)}')
        newCandle = Candle(timeVal,float(tc[1]),float(tc[2]),float(tc[3]),float(tc[4]),float(tc[5]))
        candle300s[newCandle.time] = newCandle
        newCandle.printC()
        
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