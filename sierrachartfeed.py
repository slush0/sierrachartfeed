#!/usr/bin/python
'''
Created on 14.04.2011

@author: slush
@licence: Public domain
@version 0.5
'''

from optparse import OptionParser
from itertools import takewhile, dropwhile
from datetime import datetime
import time
import os
import sys
import urllib2
import socket
import collections

try:
    import simplejson as json
except ImportError:
    import json

from scid import ScidFile, ScidRecord

BITCOINCHARTS_TRADES_URL = 'http://api.bitcoincharts.com/v1/trades.csv'
BITCOINCHARTS_SOCKET = ('bitcoincharts.com', 8002)

def bitcoincharts_history(symbol, from_timestamp, volume_precision, history_length, log=False):
    # if there is no previous local history, default to downloading
    # history_length days instead of the whole history
    if from_timestamp == 0:
        from_timestamp = int(time.time()) - history_length * 24 * 60 * 60

    oldest = int(time.time())
    history = collections.deque()

    def extract_timestamp(t):
        return int(t.split(',')[0])

    def request(start, end, symbol):
        url = '%s?start=%s&end=%s&symbol=%s' % (BITCOINCHARTS_TRADES_URL, start, end, symbol)
        req = urllib2.Request(url)
        chunk = urllib2.urlopen(req).read().strip()
        return chunk

    # initially request either a day's worth of data or all the data up to the
    # present, whichever is less 
    timespan = min([24 * 60 * 60, oldest - from_timestamp])

    # download history in chunks until we reach the chunk containing the
    # earliest wanted timestamp
    while oldest > from_timestamp:
        while True:
            try:
                chunk = request(oldest - timespan, oldest, symbol)

                if not chunk:
                    if log:
                        print "Empty chunk received, retrying..."
                    oldest -= timespan
                    timespan *= 2
                    continue
            except urllib2.HTTPError as e:
                if log:
                    print "HTTP error: {}, retrying...".format(e.code)
                if timespan > 60:
                    timespan /= 2
                time.sleep(5)
            else:
                break

        chunk = chunk.split('\n')

        if log:
            print "Fetched {} trades (end={} [{}])".format(len(chunk), oldest,
                                                           datetime.fromtimestamp(oldest))

        # generator to filter out empty lines (if any)
        trades = (i for i in reversed(chunk) if i)

        while True:
            try:
                # find the oldest timestamp in the current chunk
                oldest = extract_timestamp(next(trades))
            except ValueError:
                print oldest
                print "Corrupted timestamp detected for symbol %s, skipping" % symbol
                continue
            else:
                break

        # If we have received more than one unique timestamp, drop trades with
        # the oldest timestamp since we might not have received all trades
        # containing it and thus would end up ignoring some trades; we will
        # re-request this timestamp in the next chunk.  Otherwise, increase the
        # timespan since we need at least two unique timestamps to guarantee we
        # won't miss trades.
        if len(set(extract_timestamp(trade) for trade in chunk)) > 1:
            chunk = takewhile(lambda t: extract_timestamp(t) != oldest, chunk)
        else:
            timespan *= 2

        history.extendleft(chunk)

    # filter out trades older than the earliest wanted timestamp (if any)
    history = list(dropwhile(lambda t: extract_timestamp(t) < from_timestamp, history))

    # remove trades with the newest timestamp
    last = extract_timestamp(history[-1])
    while len(history) and extract_timestamp(history[-1]) == last:
        history.pop()

    # re-request from the last timestamp to the current time to make sure there
    # is no gap before we continue
    chunk = request(last, int(time.time()), symbol)
    if chunk:
        chunk = chunk.split("\n")
        history.extend(chunk)

    for rec in scid_from_csv(history, symbol, volume_precision, log):
        yield rec

def scid_from_csv(data, symbol, volume_precision, log=False):
    for line in data:
        if not line:
            continue
        
        line = line.split(',')
        
        try:
            timestamp, price, volume = int(line[0]), float(line[1]), int(float(line[2])*10**volume_precision)
            if log:
                print symbol, datetime.fromtimestamp(timestamp), timestamp, price, float(volume)/10**volume_precision
            yield ScidRecord(datetime.fromtimestamp(timestamp), price, price, price, price, 1, volume, 0, 0)
        except ValueError:
            print line
            print "Corrupted data for symbol %s, skipping" % symbol
        
class ScidHandler(object):
    def __init__(self, symbol, datadir, disable_history, volume_precision, history_length):
        self.symbol = symbol
        self.filename = os.path.join(datadir, "%s.scid" % symbol)
        self.volume_precision = volume_precision
        self.history_length = history_length
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
        for rec in bitcoincharts_history(self.symbol, from_timestamp, self.volume_precision, self.history_length, True):
            self.scid.write(rec.to_struct())
        self.scid.fp.flush()
         
    def ticker_update(self, data):        
        price = float(data['price'])
        volume = int(float(data['volume'])*10**self.volume_precision)
        date = datetime.fromtimestamp(float(data['timestamp']))

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
    def __init__(self, datadir, disable_history, volume_precision, history_length):
        super(ScidLoader, self).__init__() # Don't use any template dict
        
        self.datadir = datadir
        self.disable_history = disable_history
        self.volume_precision = volume_precision
        self.history_length = history_length
        
    def __getitem__(self, symbol):
        try:
            return dict.__getitem__(self, symbol)
        except KeyError:
            handler = ScidHandler(symbol, self.datadir, self.disable_history, self.volume_precision, self.history_length)
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
    parser.add_option("-l", "--history-length", default=10, dest="length", type="int",
                  help="History length to fetch in days (default is 10 days)")
    parser.add_option("-s", "--symbols", dest='symbols', default='mtgoxUSD,*',
                  help="Charts to watch, comma separated. Use * for streaming all markets.")
    parser.add_option("-b", "--bootstrap", dest="bootstrap", default=None, metavar="FILE",
                  help="Bootstrap history from a .csv file")

    (options, args) = parser.parse_args()

    if options.bootstrap:
        base_filename, _, _ = options.bootstrap.rpartition('.')
        scid_filename = "%s.scid" % base_filename

        if os.path.exists(scid_filename):
            print "{} already exists, aborting bootstrap.".format(scid_filename)
            sys.exit()

        with open(options.bootstrap) as f:
            data = f.read().split("\n")
            scid = ScidFile.create(scid_filename)
            for rec in scid_from_csv(data, base_filename, options.precision):
                scid.write(rec.to_struct())
            scid.fp.flush()
            scid.close()
            print "Bootstrap finished."
            sys.exit()

    if options.precision < 0 or options.precision > 8:
        print "Precision must be between 0 and 8"
        sys.exit()

    # Symbols to watch    
    symbols = options.symbols.split(',')
    scids = ScidLoader(options.datadir, options.disable_history, options.precision, options.length)
            
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
