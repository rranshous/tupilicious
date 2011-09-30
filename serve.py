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

# the server is going to have to be setup in such as way that it can alert
# the "waiting" connections when a new tuple gets added that matches

import pickle
import asyncore, asynchat
import sys
import socket
import logging
log = logging.getLogger()
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler(sys.stdout))

from engine import TupleEngine

TERMINATOR = '\r\n\r\n'

class TupleHandler(object):
    def __init__(self,engine):
        self.engine = engine # tuple engine

    def put(self,t):
        self.engine.put(t)

    def get(self,t):
        return self.engine.get(t)

    def get_wait(self,t):
        found = self.engine.get(t, self.handle_tuple_found)
        if found:
            return found
        return None

    def read(self):
        return self.engine.read(t)

    def read_wait(self):
        found = self.engine.read(t, self.handle_tuple_found)
        if found:
            return found
        return None

class TCPTupleHandler(asynchat.async_chat,TupleHandler):

    def __init__(self, server, sock, addr, engine):

        asynchat.async_chat.__init__(self, sock)
        TupleHandler.__init__(self, engine)

        self.server = server
        self.sock = sock
        self.addr = addr
        self.engine = engine

        # setup our terminator
        self.set_terminator(TERMINATOR)

        # data we get off the wire
        self.data = ""

        # type of actino (get,read,etc)
        self.action = None

        # do they want to wait for a match?
        self.wait = False

        # the amt of data we are to read
        self.tuple_byte_size = None

        # the tuple itself
        self.tuple_filter = None

    def collect_incoming_data(self, data):
        log.debug('got data: %s' % data)
        self.data += data

    def found_terminator(self):
        log.debug('found terminator')
        # if we don't have an action than this
        # line should have that
        if not self.action:
            # figure out what they want to do
            self.parse_action()

        elif self.action and self.tuple_byte_size:
            # see if we've received the entire tuple
            self.parse_tuple()

        else:
            # fail, we should never be here
            raise Exception()

        # see if we have enough to handle a request
        if self.action and self.tuple_filter:
            self.handle_request()

    def parse_action(self):
        # parse out the pieces
        log.debug('parsing action: %s' % self.data)
        self.action, self.tuple_byte_size = self.data.split(' ')

        # normalize the action
        self.action = self.action.lower()

        # see if it's a wait action
        if self.action.endswith('_wait'):
            self.wait = True

        # make the size an int
        self.tuple_byte_size = int(self.tuple_byte_size)

        # reset our data
        self.data = ""

    def parse_tuple(self):
        # see if we've collected enough data
        log.debug('checking parsing tuple: %s' % self.data)
        if len(self.data) >= self.tuple_byte_size:

            # decode the rep of the tuple
            log.debug('parsing')
            self.tuple_filter = (self.decode_tuple_data(self.data))

            # get rid of our data now that we've used it
            self.data = ""

    def handle_request(self):
        # call the action from our base class
        found = getattr(self, self.action)(self.tuple_filter)

        # yay we didn't even have to wait !
        if found:
            self.handle_tuple_found(found)

        elif not self.wait:
            self.push('NOT_FOUND%s' % TERMINATOR)

    def handle_tuple_found(self, t):
        # yay we found it!
        d = self.encode_tuple(t)
        self.push('FOUND %s%s%s%s' % (len(d),TERMINATOR,
                                      d,TERMINATOR))

        # clean up after ourself
        self.clean()

    def decode_tuple_data(self, data):
        return pickle.loads(data)

    def encode_tuple(self, t):
        return pickle.dumps(t)

    def clean(self):
        # we are assuming right now that requests are sent serially
        # so after each reponse we need to clean up our values
        # and wait for the next request
        self.tuple_byte_size = None
        self.wait = False
        self.action = None
        self.tuple_filter = None

        # did not clear the data because we may have begun receiving
        # more data already

class TCPServer(asyncore.dispatcher):
    def __init__(self, port, engine):
        asyncore.dispatcher.__init__(self)
        self.port = port
        self.engine = engine
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind(("", self.port))
        self.listen(5)

    def handle_accept(self):
        # pass it to handler
        conn, addr = self.accept()
        TCPTupleHandler(self, conn, addr, self.engine)


if __name__ == '__main__':
    port = sys.argv[1]
    engine = TupleEngine()
    server = TCPServer(int(port),engine)
    try:
        asyncore.loop()
    except Exception:
        server.close()
        raise
