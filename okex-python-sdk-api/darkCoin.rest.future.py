import okex.account_api as account
import okex.ett_api as ett
import okex.futures_api as future
import okex.lever_api as lever
import okex.spot_api as spot
import okex.swap_api as swap
import json,time,asyncio
import okex.utils as utils
import sys
from datetime import datetime,timedelta
sys.path.append("..")
# sys.path.extend([os.path.join(root, name) for root, dirs, _ in os.walk("../") for name in dirs])
import dmank as cfg
import db_utils as dbu
from dk_entitys import Candle

exchangeName = cfg.EXCHANGE_NAME
api_key = cfg.APIKEY
seceret_key = cfg.SCRTKEY
passphrase = cfg.PASSP
proxies = cfg.PROXIES
_instrumentId = cfg.INSTRUMENT_ID
_granularity = cfg.GRANULARITY
klineInterval = cfg.REPET_INTERVAL
buyPctGate = 0.002 #开仓阈值
amount = 1 #开仓张数

#执行Insert Delete Update
def saveLog(sql):
    # print(f'sql:{sql}')
    dbu.exeInsert(sql)

if __name__ == '__main__':

    # account api test
    # param use_server_time's value is False if is True will use server timestamp
    # accountAPI = account.AccountAPI(api_key, seceret_key, passphrase, True)
    # result = accountAPI.get_currencies()
    # result = accountAPI.get_wallet()
    # result = accountAPI.get_currency('btc')
    # result = accountAPI.get_currency('btc')
    # result = accountAPI.get_coin_fee('btc')
    # result = accountAPI.get_coin_fee('btc')
    # result = accountAPI.get_coins_withdraw_record()
    # result = accountAPI.get_coin_withdraw_record('BTC')
    # result = accountAPI.get_ledger_record_v3()
    # result = accountAPI.get_top_up_address('BTC')
    # result = accountAPI.get_top_up_address('BTC')
    # result = accountAPI.get_top_up_records()
    # result = accountAPI.get_top_up_record('BTC')

    # spot api test
    spotAPI = spot.SpotAPI(api_key, seceret_key, passphrase, True)
    # result = spotAPI.get_account_info()
    # result = spotAPI.get_coin_account_info('BTC')
    # result = spotAPI.get_ledger_record('BTC', limit=1)
    # result = spotAPI.take_order('limit', 'sell', 'BTC-USDT', 2, price='3')

    # take orders
    # params = [
    #   {"client_oid":"20180728","instrument_id":"btc-usdt","side":"sell","type":"market"," size ":"0.001"," notional ":"10001","margin_trading ":"1"},
    #   {"client_oid":"20180728","instrument_id":"btc-usdt","side":"sell","type":"limit"," size ":"0.001","notional":"10002","margin_trading ":"1"}
    # ]
    # result = spotAPI.take_orders(params)

    # result = spotAPI.revoke_order(2229535858593792, 'BTC-USDT')
    # revoke orders
    # params = [{'instrument_id': 'btc-usdt', 'orders_ids':[2233702496112640, 2233702479204352]}]
    # result = spotAPI.revoke_orders(params)
    # result = spotAPI.get_orders_list('all', 'Btc-usdT', limit=100)
    # result = spotAPI.get_order_info(2233702496112640, 'btc-usdt')
    # result = spotAPI.get_orders_pending(limit='10', froms='', to='')
    # result = spotAPI.get_fills('2234969640833024', 'btc-usdt', '', '', '')
    # result = spotAPI.get_coin_info()
    # result = spotAPI.get_depth('LTC-USDT')
    # result = spotAPI.get_ticker()
    # result = spotAPI.get_specific_ticker('LTC-USDT')
    # result = spotAPI.get_deal('LTC-USDT', 1, 3, 10)
    # result = spotAPI.get_kline('LTC-USDT', '2018-09-12T07:59:45.977Z', '2018-09-13T07:59:45.977Z', 60)
    #



    # future api test





    # result = futureAPI.get_coin_account('btc')
    # result = futureAPI.get_leverage('btc')
    # result = futureAPI.set_leverage(symbol='BTC', instrument_id='BCH-USD-181026', direction=1, leverage=10)
    # result = futureAPI.take_order()

    # take orders
    # orders = []
    # order1 = {"client_oid": "f379a96206fa4b778e1554c6dc969687", "type": "2", "price": "1800.0", "size": "1", "match_price": "0"}
    # order2 = {"client_oid": "f379a96206fa4b778e1554c6dc969687", "type": "2", "price": "1800.0", "size": "1", "match_price": "0"}
    # orders.append(order1)
    # orders.append(order2)
    # orders_data = json.dumps(orders)
    # result = futureAPI.take_orders('BCH-USD-181019', orders_data=orders_data, leverage=10)

    # result = futureAPI.get_ledger('btc')
    # result = futureAPI.get_products()
    # result = futureAPI.get_depth('BTC-USD-181019', 1)
    # result = futureAPI.get_ticker()
    # result = futureAPI.get_specific_ticker('ETC-USD-181026')
    # result = futureAPI.get_specific_ticker('ETC-USD-181026')
    # result = futureAPI.get_trades('ETC-USD-181026', 1, 3, 10)
    # result = futureAPI.get_kline('ETC-USD-181026','2018-10-14T03:48:04.081Z', '2018-10-15T03:48:04.081Z')
    # result = futureAPI.get_index('EOS-USD-181019')
    # result = futureAPI.get_products()
    # result = futureAPI.take_order("ccbce5bb7f7344288f32585cd3adf357", 'BCH-USD-181019','2','10000.1','1','0','10')
    # result = futureAPI.take_order("ccbce5bb7f7344288f32585cd3adf351",'BCH-USD-181019',2,10000.1,1,0,10)
    # result = futureAPI.get_trades('BCH-USD-181019')
    # result = futureAPI.get_rate()
    # result = futureAPI.get_estimated_price('BTC-USD-181019')
    # result = futureAPI.get_holds('BTC-USD-181019')
    # result = futureAPI.get_limit('BTC-USD-181019')
    # result = futureAPI.get_liquidation('BTC-USD-181019', 0)
    # result = futureAPI.get_holds_amount('BCH-USD-181019')
    # result = futureAPI.get_mark_price('BCH-USD-181019')

    # print('futureAPI.proxies:', proxies)
    futureAPI = future.FutureAPI(api_key, seceret_key, passphrase, True, True, proxies)
    # print('result1:', result)



    '''
    # 获取当前价
    end = utils.get_timestamp()
    # endTime = datetime.
    startT = datetime.strptime(end.split('.')[0], "%Y-%m-%dT%H:%M:%S")
    startTT = startT+timedelta(hours=-9)
    # start = '2020-03-14T08:20:06.386Z'
    start = startTT.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    print(start, end)
    klineData = futureAPI.get_kline('BTC-USDT-200327', 900, start, end)
    # print(klineData)
    print(len(klineData))
    for klS in klineData:
        print(klS)
        # for klA in klS:
            # print(klA)
    '''

    def addCandleToDB(curC):
        queryCc = dbu.exeQuery(f"select id from candles where exchange_name='{exchangeName}' and instrument_id='{_instrumentId}' and granularity='{_granularity}' and create_time='{curC.time}'")
        print('get candle from db:', queryCc)
        if queryCc == None or len(queryCc) <= 0:
            saveLog(f"insert into candles values(null,'okex','{_instrumentId}',{_granularity},'{curC.time}',{curC.open},{curC.high},{curC.low},{curC.close},{curC.volume},{curC.currency_volume});")
        else:
            saveLog(f"update candles set open={curC.open},high={curC.high},low={curC.low},close={curC.close},volume={curC.volume},currency_volume={curC.currency_volume} where id={queryCc[0][0]};")

    def getCandle(candleJson):
        tc = candleJson
        aaT = time.strptime(tc[0][0:-1],'%Y-%m-%dT%H:%M:%S.%f')
        timeVal = time.strftime("%Y-%m-%d %H:%M", aaT)
        (f'time: {tc[0]} ---- {aaT} ---- {time.strftime("%Y%m%d%H%M%S", aaT)}')
        return Candle(timeVal,float(tc[1]),float(tc[2]),float(tc[3]),float(tc[4]),float(tc[5]),float(tc[6]))

    candle300s = dict() #K线
    def candleInit():
        # 首次获取前200条kline
        endT = futureAPI._get_server_timestamp()
        curTime = utils.getTimeFromStr(endT)
        aaT = time.gmtime(curTime - _granularity * 40)
        beginT = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", aaT)
        # 时间粒度granularity必须是[60 180 300 900 1800 3600 7200 14400 21600 43200 86400 604800]中的任一值
        # 分别对应的是[1min 3min 5min 15min 30min 1hour 2hour 4hour 6hour 12hour 1day 1week]的时间段
        klineResult = futureAPI.get_kline(_instrumentId, _granularity, beginT, endT)
        #print(f"kline time: {beginT} -- {endT} >>> {klines}")
        #更新K线列表
        for tmpKl in klineResult:
            #tc = curCandle['candle']
            newCandle = getCandle(tmpKl)
            candle300s[newCandle.time] = newCandle
            newCandle.printC()
            addCandleToDB(newCandle)
        print('init candle300s count:', len(candle300s))

    def theManager():
        print('...loop begin.....chinese.输出中文...'.encode('utf8'))
        try:
            # 获取当前价
            end = futureAPI._get_server_timestamp()
            curTime = utils.getTimeFromStr(end)
            aaT = time.gmtime(curTime - _granularity * 3)
            start = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", aaT)
            # print(start, end)
            klineList = futureAPI.get_kline(_instrumentId, _granularity, start, end)
            # print(klineList)
            listCnt = len(klineList)
            print('get klineList count:', listCnt)
            if listCnt>0:
                curKline = klineList[0]
                print('new candle:', curKline)
                saveLog(f"insert into logs values(null,{time.strftime('%Y%m%d%H%M%S')},'klines','{json.dumps(curKline)}')")
            
            #"candle":["2019-04-16T10:49:00.000Z","open","high","low","close","volume"]
            #更新K线列表
            curCandle = klineList[0]
            newCandle = getCandle(curCandle)
            #已完成的k线值记入库
            addCandleToDB(newCandle)
            candle300s[newCandle.time] = newCandle
            print('[newCandle.time]:', candle300s[newCandle.time].time, 'candle300s.count:', len(candle300s))

        except Exception as ex:
            print(f"*-*-*-*-*-*-*-* app error:{ex}")
            raise 
        finally:
            print('one loop end ...')

    # asyncio.get_event_loop().run_until_complete(theManager())
   
    def loop_func(func, second):
        # 每隔second秒执行func函数
        while True:
            func()
            time.sleep(second)
    
    candleInit()
    loop_func(theManager, klineInterval)
