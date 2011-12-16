from functools import partial
import logging
import collections
import ordereddict
from decorator import decorator
log = logging.getLogger()

@decorator
def print_state(f,obj,*args,**kwargs):
    print '%s: %s' % (f.__name__,len(obj.store))
    return f(obj,*args,**kwargs)

## our engine which is actually going to track
## the tuples

class TupleEngine(object):
    def __init__(self):
        # lookup of handlers waiting for a tuple
        # the key is the pattern and the value is
        # the callback
        self.waiting = ordereddict.OrderedDict()

        # lookup of our tuples. the key is the tuple
        # the value is the # of these tuples we currently have
        self.store = {}

    @print_state
    def put(self,t):
        # a new tuple is being added
        if t not in self.store:
            log.debug('adding new tuple: %s' % str(t))
            self.store[t] = 0
        else:
            log.debug('incrementing tuple count: %s' % str(t))
            self.store[t] += 1

        # see if we can fulfill any waiting requests
        self.try_fulfill_wait()

    @print_state
    def try_fulfill_wait(self):
        # we've received a new tuple, can we make someone's day?
        for t in self.waiting.iterkeys():
            found = self.match_pattern(t)
            if found:
                try:
                    callback = self.waiting[t].popleft()
                except IndexError:
                    # there are no callbacks waiting, lets remove
                    # the key so this doesn't happen again
                    del self.waiting[t]

                # if there are no callbacks, remove the key
                if t in self.waiting and len(self.waiting[t]) == 0:
                    del self.waiting[t]

                # the callback will remove the key
                callback(found)

    @print_state
    def get(self,t,wait_callback=None):
        # they want to know if their pattern matches
        # if it does we are going to return the matching
        # tuple and remove it from our store
        # if we get passed a wait callback than if we
        # dont match we wait until we get a match and pass
        # it to the callback

        # look for a matching tuple
        found = self.match_pattern(t)
        if found:
            self._remove_tuple(found)
            return found

        # do they mind waiting ?
        elif wait_callback:
            # we want to setup the callback such that
            # it removes the tuple and the callback
            q = collections.deque()
            c = partial(self._found_callback,t,wait_callback,get=True)
            self.waiting.setdefault(t,q).append(c)
            return None


        return None

    @print_state
    def read(self,t,wait_callback=None):
        # check for a matching tuple to the passed one
        # reads dont remove the tuple, just get it
        found = self.match_pattern(t)
        if found:
            return found

        # do they mind waiting ?
        elif wait_callback:
            # we want to setup the callback such that
            # it removes the tuple and the callback
            q = collection.deque()
            c = partial(self._found_callback,t,wait_callback)
            self.waiting.setdefault(t,q).append(c)
            return None
        return None

    def _found_callback(self,t,callback,f,get=False):
        # someone was waiting for a tuple, and now it's here
        # remove the found tuple from our store if it's a get
        if get:
            self._remove_tuple(f)

        # call the callback giving it the tuple
        callback(f)

        # if we were a get and the found no longer exists
        # return False, if it still exists return True
        # this way whoever just called us knows to stop calling
        # callbacks for the find (we consumed it)
        if get:
            return False
        return True

    def match_pattern(self, t):
        # we need to look through all the tuples we are storing
        # and return the matching tuple if found
        for st in self.store.iterkeys():
            if self._match_pattern(t,st):
                return st
        return None

    def _match_pattern(self, p, t):
        # compare the pattern tuple to the stored tuple

        # if the length isn't the same they can't match
        if not len(p) == len(t):
            return False

        # each item must match if it is not none
        for po,to in zip(p,t):
            # None in pattern = wildcard
            if po is not None and not po == to:
                return False

        # they apparently match
        return True

    def _remove_tuple(self, t):
        # remove the tuple from our store
        self.store[t] -= 1
        if self.store[t] < 1:
            del self.store[t]

