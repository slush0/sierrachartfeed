#!/usr/bin/python
'''
Created on 14.04.2011

@author: slush
@licence: Public domain
@version 0.5
'''

from optparse import OptionParser
import datetime
import time
import os
import sys
import urllib2
import socket

try:
    import simplejson as json
except ImportError:
    import json

from scid import ScidFile, ScidRecord

BITCOINCHARTS_TRADES_URL = 'http://api.bitcoincharts.com/v1/trades.csv'
BITCOINCHARTS_SOCKET = ('bitcoincharts.com', 8002)
HISTORY_LENGTH = 15

def bitcoincharts_history(symbol, from_timestamp, volume_precision, log=False):
    url = '%s?start=%s&symbol=%s' % (BITCOINCHARTS_TRADES_URL, from_timestamp, symbol)
    req = urllib2.Request(url)
    for line in urllib2.urlopen(req).read().split('\n'):
        if not line:
            continue
        
        line = line.split(',')
        
        try:
            timestamp, price, volume = int(line[0]), float(line[1]), int(float(line[2])*10**volume_precision)
            if log:
                print symbol, datetime.datetime.fromtimestamp(timestamp), timestamp, price, float(volume)/10**volume_precision
            yield ScidRecord(datetime.datetime.fromtimestamp(timestamp), price, price, price, price, 1, volume, 0, 0)
        except ValueError:
            print line
            print "Corrupted data for symbol %s, skipping" % symbol
        
        
        
class ScidHandler(object):
    def __init__(self, symbol, datadir, disable_history, volume_precision):
        self.symbol = symbol
        self.filename = os.path.join(datadir, "%s.scid" % symbol)
        self.volume_precision = volume_precision
        self.load()
        if not disable_history:
            try:
                self.download_historical()
            except Exception as e:
                # We don't want to continue; if we receive new data from live feed,
                # gap inside scid file won't be filled anymore, so we must wait
                # until historical feed is available again 
                raise Exception("Historical download failed: %s, use -y to disable history" % str(e))
        
    def load(self):
        print 'Loading data file', self.filename
        if os.path.exists(self.filename):
            self.scid = ScidFile()
            self.scid.load(self.filename)
        else:
            self.scid = ScidFile.create(self.filename)    
        self.scid.seek(self.scid.length)
        
    def download_historical(self):
        length = self.scid.length
        
        if not length:
            # number of days of history * seconds per day
            from_timestamp = int(time.time() - (HISTORY_LENGTH * 86400))
        else:
            self.scid.seek(self.scid.length-1)
            rec = ScidRecord.from_struct(self.scid.readOne())
            from_timestamp = int(time.mktime(rec.DateTime.timetuple())) + 1
            
        print 'Downloading historical data'
        self.scid.seek(self.scid.length)
        for rec in bitcoincharts_history(self.symbol, from_timestamp, self.volume_precision, True):
            self.scid.write(rec.to_struct())
        self.scid.fp.flush()
         
    def ticker_update(self, data):        
        price = float(data['price'])
        volume = int(float(data['volume'])*10**self.volume_precision)
        date = datetime.datetime.fromtimestamp(float(data['timestamp']))
        
        print self.symbol, date, price, float(volume)/10**self.volume_precision
        
        # Datetime, Open, High, Low, Close, NumTrades, TotalVolume, BidVolume, AskVolume):
        try:
            rec = ScidRecord(date, price, price, price, price, 1, volume, 0, 0)
            self.scid.write(rec.to_struct())
            self.scid.fp.flush()
        except Exception as e:
            print str(e)
  
def linesplit(sock):
    buffer = ''
    while True:
        try:
            r = sock.recv(1024)
            if r == '':
                raise Exception("Socket failed")
            
            buffer = ''.join([buffer, r])
        except Exception as e:
            if str(e) != 'timed out': # Yes, there's not a better way...
                raise

        while "\n" in buffer:
            (line, buffer) = buffer.split("\n", 1)
            yield line

class ScidLoader(dict):
    def __init__(self, datadir, disable_history, volume_precision):
        super(ScidLoader, self).__init__() # Don't use any template dict
        
        self.datadir = datadir
        self.disable_history = disable_history
        self.volume_precision = volume_precision
        
    def __getitem__(self, symbol):
        try:
            return dict.__getitem__(self, symbol)
        except KeyError:
            handler = ScidHandler(symbol, self.datadir, self.disable_history, self.volume_precision)
            self[symbol] = handler
            return handler
         
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--datadir", dest="datadir", default='c:/SierraChart/data/',
                  help="Data directory of SierraChart software")
    parser.add_option("-y", "--disable-history", action='store_true', default=False,
                  help="Disable downloads from bitcoincharts.com")
    parser.add_option("-p", "--volume-precision", default=2, dest="precision", type="int",
                  help="Change decimal precision for market volume.")
    parser.add_option("-s", "--symbols", dest='symbols', default='mtgoxUSD,*',
                  help="Charts to watch, comma separated. Use * for streaming all markets.")

    (options, args) = parser.parse_args()

    if options.precision < 0 or options.precision > 8:
        print "Precision must be between 0 and 8"
        sys.exit()

    # Symbols to watch    
    symbols = options.symbols.split(',')
    scids = ScidLoader(options.datadir, options.disable_history, options.precision)
            
    for s in symbols:
        if s != '*':
            scids[s]
        
    while True:
        try:
            print "Opening streaming socket..."
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect(BITCOINCHARTS_SOCKET)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            s.send("{\"action\": \"subscribe\", \"channel\": \"tick\"}\n")
            
            for line in linesplit(s):
                rec = json.loads(line)
                if not rec['channel'].startswith('tick.'):
                    # Not a tick data
                    continue
                
                symbol = rec['channel'].rsplit('.')[1]
                if symbol not in symbols and '*' not in symbols:
                    # Filtering out symbols which user don't want to store
                    # If '*' is in symbols, don't filter anything
                    continue
                
                #print "%s: %s" % (symbol, rec['payload'])
                scids[symbol].ticker_update(rec['payload'])

        except KeyboardInterrupt:
            print "Ctrl+C detected..."
            break
        except Exception as e:
            print "%s, retrying..." % str(e)
            time.sleep(5)
            continue
        finally:
            print "Stopping streaming socket..."
            s.close()
    
    for scid in scids.values():
        scid.scid.close()
