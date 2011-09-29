

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
        found = self.engine.get(t, self.callback_handler)
        if found:
            return found
        return None

    def read(self):
        return self.engine.read(t)

    def read_wait(self):
        found = self.engine.read(t, self.callback_handler)
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
            # parse out the pieces
            self.action, self.tuple_byte_size = self.data.split(' ')
            # normalize the action
            self.action = self.action.lower()
            # make the size an int
            self.tuple_byte_size = int(self.tuple_byte_size)
            # reset our data
            self.data = ""

        elif self.action and self.tuple_byte_size:
            # see if we've collected enough data
            if len(self.data) >= self.tuple_byte_size:
                self.tuple_filter = self.decode_tuple(self.data)
                # get rid of our data now that we've used it
                self.data = ""

        else:
            # fail, we should never be here
            raise Exception()

    def decode_tuple_data(self, data):
        return pickle.load(data)

    def encode_tuple_data(self, t):
        return pickle.dump(data)
