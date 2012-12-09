'''
Created on 14.04.2011

@author: slush
@licence: Public domain
'''

from optparse import OptionParser
import datetime
import time
import asyncore
import os
import sys
import urllib2
import threading

try:
    import simplejson as json
except ImportError:
    import json

from pywsc.websocket import WebSocket
from scid import ScidFile, ScidRecord

MTGOX_WEBSOCKET_URL = 'ws://websocket.mtgox.com:80/mtgox'
BITCOINCHARTS_TRADES_URL = 'http://bitcoincharts.com/t/trades.csv'

def bitcoincharts_history(symbol, from_timestamp, log=False):
    req = urllib2.Request('%s?start=%s&end=99999999999999&symbol=%s' % (BITCOINCHARTS_TRADES_URL, from_timestamp, symbol))
    for line in urllib2.urlopen(req).read().split('\n'):
        if not line:
            continue
        
        line = line.split(',')
        
        try:
            timestamp, price, volume = int(line[0]), float(line[1]), int(float(line[2])*10**2)
            if log:
                print symbol, datetime.datetime.fromtimestamp(timestamp), timestamp, price, volume/100.
            yield ScidRecord(datetime.datetime.fromtimestamp(timestamp), price, price, price, price, volume, volume, 0, 0)
        except ValueError:
            print line
            print "Corrupted data for symbol %s, skipping" % symbol
        
        
        
class ScidHandler(object):
    def __init__(self, symbol, datadir, disable_history):
        self.symbol = symbol
        self.filename = os.path.join(datadir, "%s.scid" % symbol)
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
            from_timestamp = 0
        else:
            self.scid.seek(self.scid.length-1)
            rec = ScidRecord.from_struct(self.scid.readOne())
            from_timestamp = int(time.mktime(rec.DateTime.timetuple())) + 1
            
        print 'Downloading historical data'
        self.scid.seek(self.scid.length)
        for rec in bitcoincharts_history(self.symbol, from_timestamp, True):
            self.scid.write(rec.to_struct())
        self.scid.fp.flush()
         
    def ticker_update(self, msg):
        '''mtgox websocket ticker'''

        data = json.loads(msg)
        if data['channel'] != 'dbf1dee9-4f2e-4a08-8cb7-748919a71b21':
            return
    
        #print data
        if data.get('origin') != 'broadcast':
            return
    
        price = float(data['trade']['price'])
        volume = int(float(data['trade']['amount'])*10**2)
        date = datetime.datetime.fromtimestamp(float(data['trade']['date']))
        
        print self.symbol, date, price, volume/100.
        
        # Datetime, Open, High, Low, Close, NumTrades, TotalVolume, BidVolume, AskVolume):
        try:
            rec = ScidRecord(date, price, price, price, price, volume, volume, 0, 0)
            self.scid.write(rec.to_struct())
            self.scid.fp.flush()
        except Exception as e:
            print str(e)
  
class PollThread(threading.Thread):
    def __init__(self, askrate, scids, symbols):
        self.askrate = askrate
        self.symbols = symbols
        self.scids = scids
        self.stop = False
        super(PollThread, self).__init__()
        
    def run(self):
        from_timestamp = {}
        for symbol in self.symbols:
            scid = self.scids[symbol].scid
            if not scid.length:
                from_timestamp[symbol] = 0
            else:
                scid.seek(scid.length-1)
                rec = ScidRecord.from_struct(scid.readOne())
                from_timestamp[symbol] = int(time.mktime(rec.DateTime.timetuple()))+1
            scid.seek(scid.length)

        while not self.stop:
            for symbol in self.symbols:
                scid = self.scids[symbol].scid
                print "Updating", symbol, from_timestamp[symbol]
                
                #scid.seek(scid.length)
                change = False
                try:
                    for rec in bitcoincharts_history(symbol, from_timestamp[symbol], log=True if from_timestamp[symbol]>0 else False):
                        scid.write(rec.to_struct())
                        change = True
                                            
                    if change:
                        from_timestamp[symbol] = max(from_timestamp[symbol], int(time.mktime(rec.DateTime.timetuple()))+1)
                        scid.fp.flush()

                except Exception as e:
                    print str(e)
                    
            for i in range(int(self.askrate)):
                if self.stop:
                    break
                time.sleep(1)
            
def on_open():
    print "Connection to mtgox open"
    
def on_error(e):
    print "Some error occured on mtgox connection:", str(e)
    
def on_close():
    print "Connection closed by mtgox server"
    
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--datadir", dest="datadir", default='c:/SierraChart/data/',
                  help="Data directory of SierraChart software")
    parser.add_option("-w", "--disable-websocket", action='store_true', default=False,
                  help="Disable websocket connection to mtgox, use polling instead")
    parser.add_option("-y", "--disable-history", action='store_true', default=False,
                  help="Disable downloads from bitcoincharts.com")
    parser.add_option("-s", "--symbols", dest='symbols', default='mtgoxUSD',
                  help="Charts to watch, comma separated")
    parser.add_option("-a", "--askrate", dest='askrate', default=10,
                  help="How often poll chart data (not used for Mtgox)")

    (options, args) = parser.parse_args()

    # Symbols to watch    
    symbols = options.symbols.split(',')

    scids = {}
    for symbol in symbols:
        scids[symbol] = ScidHandler(symbol, options.datadir, options.disable_history)
        
        if not options.disable_websocket and symbol == 'mtgoxUSD':
            socket = WebSocket(MTGOX_WEBSOCKET_URL)
            socket.setEventHandlers(on_open, scids[symbol].ticker_update, on_close)  
            socket.connect()

    if not options.disable_websocket and symbol == 'mtgoxUSD':
        # Don't poll mtgox chart
        symbols.remove(symbol)
        
    t = PollThread(max(5, options.askrate), scids, symbols)
    t.daemon = True
    t.start()
    
    try:
        if not options.disable_websocket and 'mtgoxUSD' in options.symbols:
            asyncore.loop()
            
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        #socket.close()
        if not options.disable_websocket and 'mtgoxUSD' in options.symbols:
            print "Connection to mtgox closed"
        
    print "Stopping polling thread"
    t.stop = True
    t.join()
    
    for scid in scids.values():
        scid.scid.close()

    # Terminate asyncore internals        
    sys.exit()