
## for now the server can not have two outstanding
## requets on the same connection
## This will be changed in later versions of the server
## and than the client

## we are going to create a tuplicious client
## using asyncore

TERMINATOR = '\r\n\r\n'

from functools import partial
import asynchat, asyncore


class TupilicousProtocolHandler(asynchat.async_chat):
    def __init__(self, sock, addr, request_queue):
        asynchat.async_chat.__init__(self, sock)
        self.set_terminator(TERMINATOR)

        # since we only want one request out at a time
        # we need to know when we want to write
        self.mid_request = False

        # queue for requests to make
        self.request_queue = request_queue

        # how big is oiur response data?
        self.response_size = None

        # when we have a response where do we throw it
        self.response_callback = None

    def collect_incoming_data(self, data):
        self.data = self.data + data

    def found_terminator(self):
        # we've gotta line! if we don't have
        # a data size than it's the first line!
        if not self.response_size:
            pieces = self.data.split(' ')
            # clear the data out
            self.data = ''
            # see what we've found
            self.found = (pieces)[0].lower() == 'found')

            # did we find something ?
            if found:
                self.response_size = pieces[1]
            else:
                # Not Found! we're done
                self.response_callback(None)
                self.clear()

        else:
            # this must be the response data
            t = self.decode_tupe_data(self.data)
            self.data = ''
            self.response_callback(t)
            self.clear()

    def clear(self):
        self.response_callback = None
        self.data = ''
        self.mid_request = False
        self.response_size = None

    def try_pash_request(self):
        # if we are in mid request no go
        if self.mid_request:
            return False

        # try and pull something of the queue
        try:
            action, t, callback = self.request_queue.popleft()
        except IndexError:
            # empty, no go
            return False

        # have a request! push it down the wire
        self.response_callback = callback
        self.push_request(action,t)

        # we are now mid request
        self.mid_request = True

        return True


    def push_request(self, action, t):
        # push our request out on the wire
        d = self.encode_tuple(t)
        s.push('%s %s%s%s%s' % (self.action.upper(), len(d),
                                TERMINATOR, d, TERMINATOR))


class TupiliciousHandler(asyncore.dispatcher):
    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)

        # create our socket
        self.create_socket(socket.ATF_INET, socket.STOCK_STREAM)

        # connect to the server
        self.connect((host,port))

        # request queue, only one at a time
        self.requests = deque()

        # the handler protocol handler
        self.handler = None

    def make_request(self, action, t, callback):
        # put it on the queue of requests
        my_callback = partial(self.handle_response,action,t,callback)
        self.requests.append((action, t, my_callback))

    def handle_response(self, response, action, t, callback):
        # the protocol handler found a response for us
        # push  it to the callback
        callback(response)

    def handle_accept(self):
        conn, addr = self.accept()
        # now we have an open connection, we need someone to handle it
        self.handler = TupiliciousProtocolHandler(conn,addr, self.requests)


class AsyncClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def make_request(self, action, t, callback):
        request_handler = TupilicousHandler(host,port)
        request_handler.make_request(action,t,callback)
