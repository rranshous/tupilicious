import pickle

## we are going to create a simple protocol for our for actions
# put - Put something into the space
# get - Retrieve something from the space to be processed, i.e. nobody else
#       can then retrieve or read it
# read - As for get, but a copy stays in the space for others to read, or get
# get_wait - like get but waits for a match
# read_wait - like read but waits for a match

## requests
# PUT <bytes>\r\n<data>\r\n
# GET <bytes>\r\n<data>\r\n
# GET_WAIT <bytes>\r\n<data>\r\n
# READ <bytes>\r\n<data>\r\n
# READ_WAIT <bytes>\r\n<data>\r\n

## responses
# NOT_FOUND\r\n
# FOUND <bytes> <data>\r\n

# the server is going to have to be setup in such as way that it can alert
# the "waiting" connections when a new tuple gets added that matches


class TupleHandler(self):
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

class TCPTupleHandler(self):

    def __init__(self, server, sock, addr, engine):
        self.server = server
        self.sock = sock
        self.addr = addr
        self.engine = engine

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
        self.data += data

    def found_terminator(self):
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
        if len(self.data) >= self.tuple_byte_size:

            # decode the rep of the tuple
            self.tuple_filter = self.decode_tuple(self.data)

            # get rid of our data now that we've used it
            self.data = ""

    def handle_request(self):
        # call the action from our base class
        f = getattr(self, self.action)

        # yay we didn't even have to wait !
        if found:
            self.handle_tuple_found(found)

        elif not self.wait:
            self.push('NOT_FOUND\r\n')

    def handle_tuple_found(self, t):
        # yay we found it!
        d = self.encode_tuple(t)
        self.push('FOUND %s %s\r\n' % (len(d),d))

        # clean up after ourself
        self.clean()

    def decode_tuple_data(self, data):
        return pickle.load(data)

    def encode_tuple(self, t):
        return pickle.dump(data)

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
