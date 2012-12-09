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

import socket
import urlparse
from pywsc.handshake import Handshake
from pywsc.receiver import Receiver

class WebSocket(object):
    '''
    WebSocket connection
    '''
    connected = False

    def __init__(self, url, protocol=0):
        self.handshake = Handshake(url, protocol)
        self.url = url
        
    def setEventHandlers(self, onOpen, onMessage, onClose):
        self.onOpen = onOpen
        self.onMessage = onMessage
        self.onClose = onClose
        
    def connect(self):
        self._createSocket()
        self.connected = True
        handshake = self.handshake.getHandshake()
        self._sendRaw(handshake)
        handshakeComplete = False
        header = True
        buffer = ""
        serverResponse = []
        handshakeLines = []
        while not handshakeComplete:
            b = self.socket.recv(1);
            buffer += b
            if not header:
                serverResponse.extend(b);
                if len(serverResponse) >= 16:
                    handshakeComplete = True;
            elif buffer[-1:] == '\x0a' and buffer[-2:-1] == '\x0d':
                if buffer.strip() == "":
                    header = False
                else:
                    handshakeLines.extend(buffer.strip());
                buffer = ""
        receiver = Receiver(self, self.socket)
        receiver.start()
        self.onOpen()
        
    def send(self, data):
        if self.connected:
            self._sendRaw('\x00' + data + '\xff')
        else:
            print "error send" # TODO: error handling
        
    def close(self):
        self._sendCloseHandshake()
        self.socket.close()
        self.connected = False  
        
    def _sendCloseHandshake(self):
        if self.connected:
            self._sendRaw(0xff00)
        else:
            print "error sendCloseHandshake" # TODO: error handling
         
    def _sendRaw(self, data):
        self.socket.sendall(data)
        
    def _createSocket(self):
        urlparse.uses_netloc.append("ws")
        urlParts = urlparse.urlparse(self.url)
        host = urlParts.hostname
        port = urlParts.port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
