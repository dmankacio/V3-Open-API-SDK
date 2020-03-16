#!/usr/bin/env python

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

conn = sqlite3.connect('darkCoin.db')
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



# subscribe channel without login
#
# swap/ticker // 行情数据频道
# swap/candle60s // 1分钟k线数据频道
# swap/candle180s // 3分钟k线数据频道
# swap/candle300s // 5分钟k线数据频道
# swap/candle900s // 15分钟k线数据频道
# swap/candle1800s // 30分钟k线数据频道
# swap/candle3600s // 1小时k线数据频道
# swap/candle7200s // 2小时k线数据频道
# swap/candle14400s // 4小时k线数据频道
# swap/candle21600 // 6小时k线数据频道
# swap/candle43200s // 12小时k线数据频道
# swap/candle86400s // 1day k线数据频道
# swap/candle604800s // 1week k线数据频道
# swap/trade // 交易信息频道
# swap/funding_rate//资金费率频道
# swap/price_range//限价范围频道
# swap/depth //深度数据频道，首次200档，后续增量
# swap/depth5 //深度数据频道，每次返回前5档
# swap/mark_price// 标记价格频道
async def subscribe_without_login(url, channels):
    async with websockets.connect(url) as websocket:
        sub_param = {"op": "subscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await  websocket.send(sub_str)
        print(f"send: {sub_str}")

        print("receive:")
        res = await websocket.recv()
        res = inflate(res)
        print(f"{res}")

        res = await websocket.recv()
        res = inflate(res)
        print(f"{res}")

# subscribe channel need login
#
# swap/account //用户账户信息频道
# swap/position //用户持仓信息频道
# swap/order //用户交易数据频道
async def subscribe(url, api_key, passphrase, secret_key, channels):
    async with websockets.connect(url) as websocket:
        # login
        timestamp = str(server_timestamp())
        login_str = login_params(str(timestamp), api_key, passphrase, secret_key)
        await websocket.send(login_str)

        login_res = await websocket.recv()
        # print(f"receive < {login_res}")

        sub_param = {"op": "subscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await  websocket.send(sub_str)
        print(f"send: {sub_str}")

        print("receive:")
        res = await websocket.recv()
        res = inflate(res)
        print(f"{res}")

        res = await websocket.recv()
        res = inflate(res)
        print(f"{res}")

# unsubscribe channels
async def unsubscribe(url, api_key, passphrase, secret_key, channels):
    async with websockets.connect(url) as websocket:
        timestamp = str(server_timestamp())

        login_str = login_params(str(timestamp), api_key, passphrase, secret_key)

        await websocket.send(login_str)

        greeting = await websocket.recv()
        # print(f"receive < {greeting}")

        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await  websocket.send(sub_str)
        print(f"send: {sub_str}")

        res = await websocket.recv()
        res = inflate(res)
        print(f"server recv: {res}")

# unsubscribe channels
async def unsubscribe_without_login(url, channels):
    async with websockets.connect(url) as websocket:
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await  websocket.send(sub_str)
        print(f"send: {sub_str}")

        res = await websocket.recv()
        rest = inflate(res)
        print(f"server recv: {rest}")


def buildMySign(params,secretKey):
    sign = ''
    for key in sorted(params.keys()):
        sign += key + '=' + str(params[key]) +'&'
    return  hashlib.md5((sign+'secret_key='+secretKey).encode("utf-8")).hexdigest().upper()

# trade for future
def futureTrade(api_key,secretkey,symbol,contractType,price='',amount='',tradeType='',matchPrice='',leverRate=''):
    params = {
      'api_key':api_key,
      'symbol':symbol,
      'contract_type':contractType,
      'amount':amount,
      'type':tradeType,
      'match_price':matchPrice,
      'lever_rate':leverRate
    }
    if price:
        params['price'] = price
    sign = buildMySign(params,secretkey)
    finalStr = "{'event':'addChannel','channel':'ok_futuresusd_trade','parameters':{'api_key':'"+api_key+"',\
               'sign':'"+sign+"','symbol':'"+symbol+"','contract_type':'"+contractType+"'"
    if price:
        finalStr += ",'price':'"+price+"'"
    finalStr += ",'amount':'"+amount+"','type':'"+tradeType+"','match_price':'"+matchPrice+"','lever_rate':'"+leverRate+"'},'binary':'true'}"
    return finalStr

newPrice = dict() #最新价
myAccount = dict() #结构：{'instrument_id':{swap/account}}
myPositionA = dict() #多仓
myPositionB = dict() #空仓
upOrderPair = dict() #多单对，当前价上下各浮动n%同时开多和平多，一边成交则撤单另一个 {'自定ID':'id','a单':{},'b单':{}}
downOrderPair = dict() #空单对
buyPctGate = 0.002 #开仓阈值
marginRatioGate =  0.12 #保证金率阈值，低于此值停止开单| 保证金率=（账户余额+已实现盈亏+未实现盈亏）／（面值*张数／最新标记价格+冻结保证金*杠杆倍数）
amount = 1 #开仓张数
sleepGate = 0.06 #下单延时阈值
positionGuardGate = 0.1 #持仓差异守护：为防止多空单持仓的差异过大，增加守护逻辑，超过阈值时停止数量多一边的加仓，量少的一边不停。

api_key = ''
seceret_key = ''
passphrase = ''
url = 'wss://real.okex.com:10442/ws/v3'
channels = ["swap/account:EOS-USD-SWAP", "swap/position:EOS-USD-SWAP", "swap/order:EOS-USD-SWAP", "swap/ticker:EOS-USD-SWAP"]

import websocket,time
import okex.swap_api as swap
swapAPI = swap.SwapAPI(api_key, seceret_key, passphrase, True)

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
    marginRatio = float(myAccount[instrumentId]['margin_ratio']) #保证金率
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

#根据当前价匹配最适价格
def matchPrice(orderPrice, curNewPrice):
    if 'last' not in curNewPrice:
        #print('matchPrice --- last not in curNewPrice')
        return orderPrice
    else:
        last = float(curNewPrice['last'])
        best_bid = float(curNewPrice['best_bid'])
        best_ask = float(curNewPrice['best_ask'])
        if best_bid <= last and last <= best_ask:
            #print(f'matchPrice --- curPrice = last {last}')
            return last
        else:
            #print(f'matchPrice --- curPrice = last ({best_bid} + {best_ask}) / 2')
            return (best_bid + best_ask) / 2

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
        if resTbl == 'swap/ticker': #最新价
            #{"table": "swap/ticker","data": 
            #[{"best_ask": "卖一","best_bid": "买一","last": "最新","high_24h": "5.7","low_24h": "5","timestamp": "2019-05-06T06:45:56.716Z","volume_24h": "1538076","instrument_id": "BTC-USD-SWAP"}]}
            #更新账户状态
            for lastPrice in resData:
                newPrice[lastPrice['instrument_id']] = lastPrice
                #print(f"{time.strftime('%H:%M:%S')} new Price {lastPrice['last']} - {lastPrice['best_ask']}/{lastPrice['best_bid']} ")
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
        if resTbl == 'swap/order': #订单
            #state 订单状态("-2":失败,"-1":撤单成功,"0":等待成交 ,"1":部分成交, "2":完全成交,"3":下单中,"4":撤单中,）
            #type 1:开多 2:开空 3:平多 4:平空
            #print(f"order: {resData}")
            for order in resData:
                #curPrice = float(newPrice['last'] if 'last' in newPrice else order['price'])
                curPrice = matchPrice(float(order['price']), newPrice[order['instrument_id']])
                print(f"order.curPrice: {curPrice}, orderPrice: {order['price']}")
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
                    # 开新单
                    takeOrder(order['instrument_id'], thisPosition, curPrice, inPrice, outPrice, odrTypeA, odrTypeB, actionType)
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
    timestamp = str(server_timestamp())
    login_str = login_params(str(timestamp), api_key, passphrase, seceret_key)
    print(f"login: {login_str}")
    ws.send(login_str)
    saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'login','user login')")


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