'''
Created on 7.1.2010

@author: slush
@licence: Public domain

File sctructure:

The intraday data file header, s_Header, is 56 bytes in size.

struct s_Header
{
    char FileTypeUniqueHeaderID[4];  // Set to "SCID"
    unsigned long HeaderSize; // Automatically set to the header size in bytes
 
    unsigned long RecordSize; // Automatically set to the  record size in bytes
    unsigned short Version; // Automatically set to the correct version.
    unsigned short Unused1; // Not used
 
    unsigned long UTCStartIndex;  // By default this is set to the correct value and it should not be changed
 
    char Reserve[36]; // Not used
 
    s_Header()
    {
        strncpy (FileTypeUniqueHeaderID, "SCID", 4);
        HeaderSize = sizeof(s_Header);
        RecordSize = sizeof(s_Record);
        Version = 1;
        Unused1 = 0;
        UTCStartIndex = 0;
        memset(Reserve, 0, sizeof(Reserve));
    }
};

The intraday data file header, s_IntradayRecord, is 40 bytes in size.

struct s_IntradayRecord
{
    double DateTime;
    
    float Open;
    float High;
    float Low;
    float Close;
    
    unsigned long NumTrades;
    unsigned long TotalVolume;
    unsigned long BidVolume;
    unsigned long AskVolume;
        
    
    s_IntradayRecord()
    {
        DateTime=0.0;
        Open = 0.0;
        High = 0.0;
        Low = 0.0;
        Close = 0.0;
        NumTrades = 0;
        TotalVolume = 0;
        BidVolume = 0;
        AskVolume = 0;
    }
        

};
'''

import struct
import os
import datetime

from scidexception import ScidException
from scidrecord import ScidRecord, RECORD_STRUCT

HEADER_STRUCT = struct.Struct('4sLLHHL36s')
 
class ScidFile(object):
    def __init__(self, zone=0):
        self.fp = None
        self.zone_user = zone
        self.zone = None
       
    @staticmethod
    def create(filename, zone=0):
        f = open(filename, 'wb')
        f.write(HEADER_STRUCT.pack('SCID', 56, 40, 1, 0, zone, '\x00' * 36))
        f.close()
        s = ScidFile()
        s.load(filename)
        return s
    
    def load(self, filename):
        self.close()        
        self.fp = open(filename, 'r+b')
        
        header = self.fp.read(HEADER_STRUCT.size)
        if len(header) != HEADER_STRUCT.size:
            raise ScidException('Header is too short')
        h = HEADER_STRUCT.unpack(header)
        if h[0] != 'SCID':
            raise ScidException('Missing SCID constant in header')
        if h[1] != HEADER_STRUCT.size:
            raise ScidException('Declared size of header structure differs from known specification')
        if h[2] != RECORD_STRUCT.size:
            raise ScidException('Declared size of record structure differs from known specification')
        if h[3] != 1:
            raise ScidException('Different protocol version')
        
        self.zone = datetime.timedelta(hours=self.zone_user-h[5]) # S vypoctem si nejsem moc jisty O:-)
            
        self.updateLength()
        self.seek(0) # First record
        
    def close(self):
        if self.fp: self.fp.close()

    def updateLength(self):
        pos = self.fp.tell()
        self.fp.seek(0, os.SEEK_END) # Get count of records 
        self.length = self.tell()
        self.fp.seek(pos, os.SEEK_SET)

    def tell(self):
        return self._pos2record(self.fp.tell())
        
    def seek(self, recnum):
        if recnum < 0 or recnum > self.length:
            raise ScidException('Seeking out of bounds')
        self.fp.seek(HEADER_STRUCT.size + recnum * RECORD_STRUCT.size)
    
    def seekTime(self, dt, exact=False):
        min = 0
        max = self.length-1
        
        dt = dt-self.zone
 
        '''
        min_time = readTime(min)
        max_time = readTime(max)
        cur_time = min_time
        
            cur_dt_sec = (dt - cur_time)
            cur_dt_sec = abs(cur_dt_sec.days*86400 + cur_dt_sec.seconds)
            #print cur_dt_sec
                    
            if side>0: cur_minmax_sec = (max_time - cur_time)
            else: cur_minmax_sec = (cur_time - min_time)
            cur_minmax_sec = cur_minmax_sec.days*86400 + cur_minmax_sec.seconds
            #print cur_minmax_sec
            
            move_proc = cur_dt_sec/float(cur_minmax_sec)
            if side < 0: move_proc = 1 - move_proc
            print "move", move_proc
            cur_pos = int(min+(max-min)*move_proc)
            print "xxx", max-min, cur_minmax_sec, cur_dt_sec, cur_pos
            #print "CUR POS", cur_pos
            
        '''
        
        for i in range(50):
            cur_pos = min + (max - min) / 2
            
            if max-min < 1:
                break
            
            if max-min == 1:
                cur_pos += 1
                min += 1
                
            #print "seeking", cur_pos, max-min,
            self.seek(cur_pos)
            cur_time = ScidRecord.from_struct(self.readOne()).DateTime
            
            #print dt - cur_time
            if dt > cur_time:
                min = cur_pos
            else:
                max = cur_pos
        
        if exact and cur_time != dt: raise Exception("Unable to find exact time")
        
        self.seek(cur_pos)
        #print i
        
    def _pos2record(self, pos):
        return (pos - HEADER_STRUCT.size) / RECORD_STRUCT.size
        
    def _record2pos(self, record):
        return record * RECORD_STRUCT.size + HEADER_STRUCT.size

    def readOne(self):
        try:
            data = self.fp.read(RECORD_STRUCT.size)
            record = RECORD_STRUCT.unpack(data)
        except Exception, e:
            if self.fp.tell() >= self._record2pos(self.length):
                raise ScidException('End of file reached')
            else:
                raise e
        
        return record#ScidRecord.from_struct(record)  
            
    def readIter(self, limit=None):
        batch = 1000
        remain = self.length - self.tell()
        record_struct_size = RECORD_STRUCT.size    
        i = 0 
        while True:
            if batch > remain:
                batch = remain
            
            if limit and limit <= i or batch == 0:
                return
            
            remain -= batch
            i+=batch
            
            data = self.fp.read(record_struct_size * batch)
            for x in range(batch):
                #yield ScidRecord.from_struct()
                yield RECORD_STRUCT.unpack_from(data, x*record_struct_size)
            
    def write(self, record):
        self.fp.write(RECORD_STRUCT.pack(*record))
        #self.fp.write(record.to_struct())