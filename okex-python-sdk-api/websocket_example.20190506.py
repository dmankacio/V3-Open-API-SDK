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

async def heartBeat():
    async with websockets.connect(url) as websocket:
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        sub_str = 'ping'
        await  websocket.send(sub_str)
        print(f"send: {sub_str}")

        res = await websocket.recv()
        print(f"server recv.res: {res}")
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

myAccount = dict()
myPositionA = dict() #多仓
myPositionB = dict() #空仓
upOrderPair = dict() #多单对，当前价上下各浮动n%同时开多和平多，一边成交则撤单另一个 {'自定ID':'id','a单':{},'b单':{}}
upAOrders = dict() #开多
upBOrders = dict() #平多
downAOrders = dict() #开空
downBOrders = dict() #平空
buyPctGate = 0.003 #开仓阈值
marginRatioGate =  0.08 #保证金率阈值，低于此值停止开单| 保证金率=（账户余额+已实现盈亏+未实现盈亏）／（面值*张数／最新标记价格+冻结保证金*杠杆倍数）
amount = 1 #开仓张数

api_key = ''
seceret_key = ''
passphrase = ''
url = 'wss://real.okex.com:10442/ws/v3'
channels = ["swap/account:EOS-USD-SWAP", "swap/position:EOS-USD-SWAP", "swap/order:EOS-USD-SWAP"]

import websocket,time
import okex.swap_api as swap
swapAPI = swap.SwapAPI(api_key, seceret_key, passphrase, True)

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
            print(f"login succesfull subscribe...send: {sub_str}")
        elif evt == 'error':
            print(f"sth err, errorCode: {resDict['errorCode']}, message: {resDict['message']}")
        elif evt == 'subscribe':
            print(f"subscribe succesfull, channel: {resDict['channel']}")
        else:
            print(f"unknow event: {resDict['event']}")
    elif 'table' in resDict:
        resTbl = resDict['table']
        resData = resDict['data'] if 'data' in resDict else {}
        if resTbl == 'swap/account': #账户状态
            print(f"account: {resData}")
            #更新账户状态
            for curOne in resData:
                myAccount[curOne['instrument_id']] = curOne
            print(f"new myAccount: {myAccount}")
        if resTbl == 'swap/position': #持仓
            print(f"position: {resData}")
            for curOne in resData:
                for holdT in curOne['holding']:
                    if holdT['side'] == 'long':
                        myPositionA[curOne['instrument_id']] = holdT
                    else:
                        myPositionB[curOne['instrument_id']] = holdT
            print(f"new positionA: {myPositionA}")
            print(f"new positionB: {myPositionB}")
        if resTbl == 'swap/order': #订单
            #state 订单状态("-2":失败,"-1":撤单成功,"0":等待成交 ,"1":部分成交, "2":完全成交,"3":下单中,"4":撤单中,）
            #type 1:开多 2:开空 3:平多 4:平空
            print(f"order: {resData}")
            for order in resData:
                if order['type'] == '1' or order['type'] == '3':
                    if order['state'] == '0': #下单成功
                        tId = order['client_oid']
                        if tId in upOrderPair:
                            if order['type'] == '1':
                                upOrderPair[tId]['a'] = order
                            else:
                                upOrderPair[tId]['b'] = order
                        else:                            
                            tA = order if order['type'] == '1' else None
                            tB = order if order['type'] == '3' else None
                            upOrderPair[tId] = {'a': tA, 'b': tB}
                        print(f"update upOrderPair: {upOrderPair}")
                    elif order['state'] == '2': #挂单成交
                        upAOrders[order['order_id']] = None
                        # 开多成功，撤旧单，挂新单平多、开多
                        curPrice = float(order['price'])
                        clientId = order['client_oid'] if 'client_oid' in order else None
                        if clientId is not None:
                            curPair = upOrderPair[clientId] if clientId in upOrderPair else None
                            if curPair is not None:
                                print(f'------------ curPair: {curPair}')
                                orderA = curPair['a']
                                orderB = curPair['b']
                                revokeOid = None
                                #撤单
                                if orderA is not None and orderA['state'] == '0':                                    
                                    revokeOid = orderA['order_id']
                                elif orderB is not None and orderB['state'] == '0':
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
                        
                        # 开新单
                        if myAccount is not None:
                            marginRatio = float(myAccount[order['instrument_id']]['margin_ratio'])
                            clientOid = 'dk' + time.strftime('%Y%m%d%H%M%S') 
                            if marginRatio > marginRatioGate:
                                newUpPrice = curPrice * (1+buyPctGate)
                                newDownPrice = curPrice * (1-buyPctGate)
                                #client_oid 是数字+字母（大小写）或者纯字母（大小写）类型 1-32位
                                print(f'take_orders clientOid: {clientOid}, curPrice: {curPrice}, newUpPrice: {newUpPrice}, newDownPrice: {newDownPrice}')
                                #print('take_orders:', "('EOS-USD-SWAP', {clientOid}, '1', '4.1', '1001', '')")
                                result = swapAPI.take_orders([
                                    {"client_oid": clientOid,"price": str(newDownPrice),"size": amount,"type": "1","match_price": "0"},
                                    {"client_oid": clientOid,"price": str(newUpPrice),"size": amount,"type": "3","match_price": "0"}
                                ],order['instrument_id'])
                                print(f'take_orders.result: {result}')
                            else: #保证金低，只平多
                                curPosition = myPositionA[order['instrument_id']]['avail_position']
                                if int(curPosition) > 1:
                                    print(f"stop add order, margin ratio is low: {marginRatio}")
                                    print(f"take_order: ({order['instrument_id']},{amount},'3',{str(newUpPrice)},{clientOid},'')")
                                    result = swapAPI.take_order(order['instrument_id'],amount,'3',str(newUpPrice),clientOid,'')
                                    print(f'take_orders.result: {result}')
                                else:
                                    print(f'long position is low: {curPosition}')
                    elif order['state'] == '-1':
                        #撤单成功
                        print(f"revoke order successfull clientOid:{order['client_oid']}, orderId:{order['order_id']}")
                    else:
                        print(f"unknow order state: {order['state']}")
                else:
                    print(f"order.type: {order['type']}, order: {order}")
            (f"new upAOrders: {upAOrders}")
            # print(f"new upBOrders: {upBOrders}")
            # print(f"new downAOrders: {downAOrders}")
            # print(f"new downBOrders: {downBOrders}")
    else:
        print(f'unknow result: {resDict}')

def on_error(ws, error):
    # print(ws)
    print(error)

def on_close(ws):
    # print(ws)
    print("### closed ###")
    time.sleep(3)
    print("### reconnect... ###")
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

    #login_res = await websocket.recv()
    # print(f"receive < {login_res}")


async def theManager(url):
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever(ping_interval=29, ping_timeout=5)
    print('........chinese.输出中文...'.encode('utf8'))


# asyncio.get_event_loop().run_until_complete(login(url, api_key, passphrase, seceret_key))


# asyncio.get_event_loop().run_until_complete(subscribe(url, api_key, passphrase, seceret_key, channels))
# asyncio.get_event_loop().run_until_complete(unsubscribe(url, api_key, passphrase, seceret_key, channels))
# asyncio.get_event_loop().run_until_complete(subscribe_without_login(url, channels))
# asyncio.get_event_loop().run_until_complete(unsubscribe_without_login(url, channels))
# asyncio.get_event_loop().run_until_complete(heartBeat(url, channels))
asyncio.get_event_loop().run_until_complete(theManager(url))