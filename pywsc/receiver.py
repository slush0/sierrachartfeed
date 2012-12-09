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

from threading import Thread

class Receiver(Thread):
    '''
    WebSocket receiver
    '''

    def __init__(self, websocket, socket):
        Thread.__init__(self)
        self.socket = socket
        self.websocket = websocket
        
    def run(self):
        frameStart = False
        message = ""
        while 1:
            b = self.socket.recv(1)
            if not b:
                self._handleError()
                break
            if b == '\x00':
                frameStart = True
            elif b == '\xff' and frameStart == True:
                frameStart = False
                self.websocket.onMessage(message)
                message = ""
            elif frameStart == True:
                message += b
            else:
                print "error receiver" # TODO: error handling
        
    def _handleError(self):
        pass # TODO: implement
    