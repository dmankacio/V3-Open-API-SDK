# -*- coding: UTF-8 -*-

class Candle(object):
    def __init__(self, time, open, high, low, close, volume, currency_volume):
        self.time = time    #开始时间
        self.open = open    #开盘价格
        self.high = high    #最高价格
        self.low = low  #最低价格
        self.close = close  #收盘价
        self.volume = volume    #交易量（张）
        self.currency_volume = currency_volume  #按币种折算的交易量
    def printC(self):
        print(f"Candle: {self.time} >> {self.open}/{self.high}/{self.low}/{self.close}/{self.volume}/{self.currency_volume} ")
