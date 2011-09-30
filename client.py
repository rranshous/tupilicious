## we are going to create a simple protocol for our for actions
# put - Put something into the space
# get - Retrieve something from the space to be processed, i.e. nobody else
#       can then retrieve or read it
# read - As for get, but a copy stays in the space for others to read, or get
# get_wait - like get but waits for a match
# read_wait - like read but waits for a match

## requests
# PUT <bytes>\r\n\r\n<data>\r\n\r\n
# GET <bytes>\r\n\r\n<data>\r\n\r\n
# GET_WAIT <bytes>\r\n\r\n<data>\r\n\r\n
# READ <bytes>\r\n\r\n<data>\r\n\r\n
# READ_WAIT <bytes>\r\n\r\n<data>\r\n\r\n

## responses
# NOT_FOUND\r\n\r\n
# FOUND <bytes>\r\n\r\n<data>\r\n\r\n

from functools import partial
import pickle
import socket
import logging
import sys
log = logging.getLogger()
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler(sys.stdout))

TERMINATOR = '\r\n\r\n'

class TCPTupleClient(object):
    def __init__(self, host, port):
        self.address = (host,port)
        self.socket = None

    def get_socket(self):
        # no unix sockets here
        if not self.socket:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(self.address)
            return s
        return self.socket

    def make_request(self, action, t):

        log.debug('making request: %s %s' % (action,t))

        # get our socket
        s = self.get_socket()

        # send our request
        d = self.encode_tuple(t)
        s.send('%s %s%s%s%s' % (action.upper(), len(d),
                                TERMINATOR, d, TERMINATOR))

        # wait for a response
        # the response is either going to be NOT_FOUND or FOUND <size>
        # so we'll read enough for FOUND and go from there
        r = s.recv(6).strip() # we consume the space .. maybe
        log.debug('initial response: %s' % r)


        # is it found ?!
        if r.lower() != 'found':
            log.debug('not found')
            # sadface, not found
            # read out the rest of the message
            s.recv(7)
            return None

        # if it's a put, we're done
        if action.lower() == 'put':
            return True

        # much excitement! lets read how much data we get
        # read until returns
        buff = ''
        while not buff.endswith(TERMINATOR):
            buff += s.recv(1)
        data_len = int(buff[:len(TERMINATOR)])
        log.debug('data len: %s' % data_len)

        # read in the tuple data
        tuple_data = s.recv(data_len)
        log.debug('tuple data: %s' % tuple_data)

        # read the newline out
        s.recv(2)

        # get our tuple
        found_tuple = self.decode_tuple_data(tuple_data)
        log.debug('found tuple: ' + str(found_tuple))
        return found_tuple

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

    def decode_tuple_data(self, data):
        return pickle.loads(data)

    def encode_tuple(self, t):
        return pickle.dumps(t)
