'''
Created on 7.1.2010

@author: slush
@licence: Public domain
'''
import datetime
import struct

OLE_TIME_ZERO = datetime.datetime(1899, 12, 30, 0, 0, 0)
RECORD_STRUCT = struct.Struct('<dffffLLLL')

class ScidRecord(object):
    def __init__(self, DateTime, Open, High, Low, Close, NumTrades, TotalVolume, BidVolume, AskVolume):
        try:
            self.DateTime = self.ole2dt(int(DateTime))
        except TypeError:
            self.DateTime = DateTime
        self.Open = Open
        self.High = High
        self.Low = Low
        self.Close = Close
        self.NumTrades = NumTrades
        self.TotalVolume = TotalVolume
        self.BidVolume = BidVolume
        self.AskVolume = AskVolume
        
    @staticmethod
    def from_struct(struct, zone=0):
        return ScidRecord(
            DateTime = ScidRecord.ole2dt(struct[0])+datetime.timedelta(hours=zone) if zone else ScidRecord.ole2dt(struct[0]),
            Open = struct[1],
            High = struct[2],
            Low = struct[3],
            Close = struct[4],
            NumTrades = struct[5],
            TotalVolume = struct[6],
            BidVolume = struct[7],
            AskVolume = struct[8],
            )
    
    def __add__(self, other):
        if isinstance(other, tuple):
            #self.Open = tuple[1]
            return ScidRecord(self.DateTime,
                              self.Open,
                              max(self.High, other[2]),
                              min(self.Low, other[3]),
                              other[4],
                              self.NumTrades + other[5],
                              self.TotalVolume + other[6],
                              self.BidVolume + other[7],
                              self.AskVolume + other[8],
                            )
        else:
            raise Exception("Adding ScidRecord not implemented yet")
    
    def __repr__(self):
        return {
            'DateTime': self.DateTime,
            'Open': self.Open,
            'High': self.High,
            'Low': self.Low,
            'Close': self.Close,
            'NumTrades': self.NumTrades,
            'TotalVolume': self.TotalVolume,
            'BidVolume': self.BidVolume,
            'AskVolume': self.AskVolume,
            }
    
    def __str__(self):
        return str(self.__repr__())
       
    def to_struct(self):
        return (self.dt2ole(self.DateTime), self.Open, self.High, self.Low, \
                            self.Close, self.NumTrades, self.TotalVolume, self.BidVolume, self.AskVolume)
    
    @staticmethod
    def ole2dt(oledt):
        return OLE_TIME_ZERO + datetime.timedelta(days=float(oledt))
        
    @staticmethod 
    def dt2ole(dt):
        delta = dt - OLE_TIME_ZERO
        return delta.days + delta.seconds / 86400.
    
