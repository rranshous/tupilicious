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

from functools import partial
import pickle

class TCPTupleClient(self):

    def make_request(self, action, t):
        # get our socket
        s = self.get_socket()

        # send our request
        d = self.encode_tuple(t)
        s.send('%s %s %s\r\n' % (action.upper(), len(d), d))

        # wait for a response
        # the response is either going to be NOT_FOUND or FOUND <size>
        # so we'll read enough for FOUND and go from there
        r = s.recv(6).strip() # we consume the space .. maybe

        # is it found ?!
        if r.lower() != 'found':
            # sadface, not found
            return None

        # much excitement! lets read how much data we get
        # read until space
        buff = ''
        while not data.endswith(' '):
            buff += s.recv(1)
        data_len = int(buff[:-1]) # don't want the space

        # read in the tuple data
        tuple_data = s.recv(data_len)

        # get our tuple
        found_tuple = self.decode_tuple_data(tuple_data)

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
        return pickle.load(data)

    def encode_tuple(self, t):
        return pickle.dump(data)
