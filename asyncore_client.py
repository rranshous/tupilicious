
## for now the server can not have two outstanding
## requets on the same connection
## This will be changed in later versions of the server
## and than the client

## we are going to create a tuplicious client
## using asyncore

from functools import partial
import asynchat, asyncore
import sys
import socket
import logging
from collections import deque
from helpers import decode_tuple_data, encode_tuple
log = logging.getLogger()
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler(sys.stdout))

TERMINATOR = '\r\n\r\n'



class TupiliciousHandler(asynchat.async_chat):
    def __init__(self, host, port):

        log.debug('TupiliciousHandler: init')

        asynchat.async_chat.__init__(self)

        # deep where we are connecting
        self.host_and_port = host,port

        # setup a queue for our requests since the server
        # doesn't handle multiple requests per connection atm
        self.requests = deque()

        # let'm know what our terminator is
        self.set_terminator(TERMINATOR)

        # since we only want one request out at a time
        # we need to know when we want to write
        self.mid_request = False

        # what request are we working on now?
        # (action,tuple)
        self.current_request = None

        # how big is oiur response data?
        self.response_size = None

        # when we have a response where do we throw it
        self.response_callback = None

        # our in buffer
        self.data = ''

        # did the response say we found something?
        self.found = None

        # connect to our host
        self.connect_host()

    def connect_host(self):
        # connects us to our tuple server
        log.debug('TupiliciousHandler: connecting')
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(self.host_and_port)

    def collect_incoming_data(self, data):
        self.data = self.data + data

    def found_terminator(self):
        # we've gotta line! if we don't have
        # a data size than it's the first line!

        log.debug('TupiliciousHandler: found terminator %s %s %s',
                  len(self.data),self.found,self.response_size)

        if not self.response_size:
            pieces = self.data.split(' ')
            # clean the data out
            self.data = ''
            # see what we've found
            self.found = (pieces[0].lower() == 'found')

            # did we find something ?
            if self.found:
                # if it was found but is a put, just
                # send the callback true
                if self.current_request[0].lower() == 'put':
                    self.response_callback(True)
                    self.clean()
                else:
                    self.response_size = pieces[1]
            else:
                # Not Found! we're done

                # if it was not found but is a put, just
                # send the callback False
                if self.current_request[0].lower() == 'put':
                    self.response_callback(False)
                    self.clean()
                else:
                    self.response_callback(None)
                    self.clean()

        else:
            # this must be the response data
            t = decode_tuple_data(self.data)
            self.data = ''
            self.response_callback(t)
            self.clean()

    def clean(self):
        log.debug('TupiliciousHandler: clean')
        self.response_callback = None
        self.data = ''
        self.mid_request = False
        self.response_size = None
        self.found = None

        # send off the next request
        self.try_push_request()

    def make_request(self, action, t, callback):
        # put it on the queue of requests
        log.debug('TupiliciousHandler: making request: %s %s %s',
                  action,t,callback)
        my_callback = partial(self.handle_response,action,t,callback)
        self.requests.append((action, t, my_callback))

        # see if we can push it now
        self.try_push_request()

    def try_push_request(self):
        log.debug('TupiliciousHandler: trying to push')

        # if we are in mid request no go
        if self.mid_request:
            log.debug('TupiliciousHandler: no push, mid request')
            return False

        # try and pull something of the queue
        try:
            action, t, callback = self.requests.popleft()
        except IndexError:
            # empty, no go
            return False

        # have a request! push it down the wire
        self.response_callback = callback
        self.push_request(action,t)

        # we are now mid request
        self.mid_request = True

        return True

    def handle_response(self, action, t, callback, response):
        log.debug('TupiliciousHandler: handling response: %s %s %s %s',
                  response, action, t, callback)
        # the protocol handler found a response for us
        # push  it to the callback
        if callback:
            callback(response)

    def push_request(self, action, t):
        # push our request out on the wire
        log.debug('TupiliciousHandler: pushing request %s %s',
                  action,t)
        d = encode_tuple(t)
        self.current_request = (action,t)
        self.push('%s %s%s%s%s' % (action.upper(), len(d),
                                   TERMINATOR, d, TERMINATOR))

    def handle_close(self):
        log.debug('TupiliciousHandler: handling close')
        self.close()


class AsyncClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.request_handler = TupiliciousHandler(self.host,self.port)
        self.request_handlers = []

    def make_request(self, action, t, callback=None):
        # if we don't have any request handlers which aren't mid request
        # fire up another
        found = None
        for rh in self.request_handlers:
            if not rh.mid_request:
                found = rh
        # TODO: cleanup extra request handlers
        if not found:
            request_handler = TupiliciousHandler(self.host,self.port)
            self.request_handlers.append(request_handler)
            found = request_handler
        found.make_request(action,t,callback)

        # cleanup request handlers
        to_close = []
        for rh in self.request_handlers:
            if not rh.mid_request and not self.requests:
                to_close.append(rh)
        for rh in to_close:
            print 'removing rh'
            rh.close()
            self.request_handlers.remove(rh)

    def __getattr__(self,a,*args):
        # instead of putting a function for each api method
        # we are cheating and simply sticking ourself into the
        # attribute lookup
        cmds = ['put','get','get_wait','read','read_wait']
        if a in cmds:
            # w00t, doing an api call ::dance::
            # return a function which calls make request
            # w/ the command we want
            return partial(self.make_request,a)

        # ::shrug::
        return super(TCPTupleClient,self).__getattr__(a,*args)

def print_response(r):
    log.debug('response: %s',str(r))

if __name__ == '__main__':
    log.debug('STARTING')
    ac = AsyncClient('localhost',9119)
    ac.put(('test',1))
    ac.get(('test',None),print_response)
    ac.put(('test',2))
    ac.read(('test',None),print_response)
    ac.read(('test',None),print_response)
    ac.read(('test',None),print_response)
    ac.get(('test',None),print_response)
    ac.get(('test',None),print_response)
    ac.get_wait(('test',None),print_response)
    asyncore.loop()
