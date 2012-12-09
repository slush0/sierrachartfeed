'''
Copyright (C) 2010 Roderick Baier

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
'''

import sys
from hashlib import md5
from random import Random
import urlparse

class Handshake(object):
    '''
    WebSocket handshake
    '''
    key1 = ""
    key2 = ""
    key3 = ""
    expectedServerResponse = ""

    def __init__(self, url, protocol):
        urlparse.uses_netloc.append("ws")
        urlParts = urlparse.urlparse(url)
        self.protocol = protocol
        self.host = urlParts.hostname
        self.port = urlParts.port
        self.origin = urlParts.hostname
        self.resource = urlParts.path
        self.generateKeys()
    
    def getHandshake(self):
        handshake = "GET " + self.resource + " HTTP/1.1\r\n" + \
                    "Host: " + self.host + "\r\n" + \
                    "Connection: Upgrade\r\n" + \
                    "Sec-WebSocket-Key2: " + self.key2 + "\r\n"
        if self.protocol != 0:
            handshake += "Sec-WebSocket-Protocol: " + self.protocol + "\r\n"
        handshake += "Upgrade: WebSocket\r\n" + \
                     "Sec-WebSocket-Key1: " + self.key1 + "\r\n" + \
                     "Origin: " + self.origin + "\r\n\r\n"
        handshake = handshake.encode('ascii')
        handshake += self.key3
        return handshake
    
    def generateKeys(self):
        rng = Random()
        spaces1 = rng.randint(1, 12)
        spaces2 = rng.randint(1, 12)
        max1 = sys.maxint / spaces1
        max2 = sys.maxint / spaces2
        number1 = rng.randint(0, max1)
        number2 = rng.randint(0, max2)
        product1 = number1 * spaces1
        product2 = number2 * spaces2
        key1 = str(product1)
        key2 = str(product2)
        key1 = self._insertRandomCharacters(key1)
        key2 = self._insertRandomCharacters(key2)
        self.key1 = self._insertSpaces(key1, spaces1)
        self.key2 = self._insertSpaces(key2, spaces2)
        self.key3 = self._createRandomBytes()
        challenge = str(number1) + str(number2) + self.key3
        self.expectedServerResponse = md5(challenge)
        
    def _insertRandomCharacters(self, key):
        rng = Random()
        count = rng.randint(1, 12)
        randomChars = []
        randCount = 0
        while (randCount < count):
            rand = int(rng.random() * 0x7e + 0x21)
            if (((0x21 < rand) and (rand < 0x2f)) or ((0x3a < rand) and (rand < 0x7e))):
                randomChars.extend(chr(rand))
                randCount += 1
        for i in xrange(0, count):
            split = rng.randint(0, len(key))
            part1 = key[0:split]
            part2 = key[split:]
            key = part1 + randomChars[i] + part2
        return key
        
    def _insertSpaces(self, key, spaces):
        rng = Random()
        for i in xrange(0, spaces):
            split = rng.randint(0, len(key))
            part1 = key[1:split]
            part2 = key[split:-1]
            key = part1 + " " + part2
        return key

    def _createRandomBytes(self):
        rng = Random()
        bytes = [chr(rng.randrange(256)) for i in range(8)]
        return "".join(bytes)
        