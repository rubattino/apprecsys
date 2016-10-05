__author__ = 'mertergun'
from math import radians, cos, sin, asin, sqrt
from collections import namedtuple
import datetime
EventRow = namedtuple("event", ["userId", "itemId","ts","city","lat","lon"])
EventRow2 = namedtuple("event", ["userId", "itemId","ts","city","lat","lon","timeofday","dayofweek"])
TrainRow = namedtuple("train", ["itemId", "context"])
ContextRow = namedtuple("context", ["ts","city", "lat", "lon", "moving", "location", "time_of_day" ])

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def loadDataset(path):
    events = sc.textFile(path).map(lambda line: line[1:-1].split(",")).map(lambda x: EventRow(
        userId=int(x[0]),
        itemId=int(x[1]),
        ts=int(x[2]),
        city=int(x[3]),
        lat=float(x[6]),
        lon=float(x[7])
            # day_of_week=datetime.fromtimestamp(int(x[2])).day,
            # hour_of_day=datetime.fromtimestamp(int(x[2])).hour
            #day_of_week=int(x[4]),
            #hour_of_day=int(x[5])
        ))
    #test = sc.textFile(path).map(lambda line: TestRow(
    #   recId=int(line[1:].split(",")[0]),
    #    userId=int(line[1:].split(",")[1]),
    #    contextForTest=ContextForTest(
    #        time_of_day=line.split(",")[2][2:-1],
    #        day_of_week=line.split(",")[3][2:-1],
    #        city=int(line.split(",")[4])
    #    ),
    #       listOfItems=[int(itemStr) for itemStr in line.split("[")[1][:-2].split(",")]
    #))
    return events

def loadDataset2(path):
    events = sc.textFile(path).map(lambda line: line[1:-1].split(",")).map(lambda x: EventRow2(
        userId=int(x[0]),
        itemId=int(x[1]),
        ts=int(x[2]),
        city=int(x[3]),
        lat=float(x[6]),
        lon=float(x[7]),
            # day_of_week=datetime.fromtimestamp(int(x[2])).day,
            # hour_of_day=datetime.fromtimestamp(int(x[2])).hour
            #day_of_week=int(x[4]),
            #hour_of_day=int(x[5])
        ))
    #test = sc.textFile(path).map(lambda line: TestRow(
    #   recId=int(line[1:].split(",")[0]),
    #    userId=int(line[1:].split(",")[1]),
    #    contextForTest=ContextForTest(
    #        time_of_day=line.split(",")[2][2:-1],
    #        day_of_week=line.split(",")[3][2:-1],
    #        city=int(line.split(",")[4])
    #    ),
    #       listOfItems=[int(itemStr) for itemStr in line.split("[")[1][:-2].split(",")]
    #))
    return events

def splitRdd(rdd,splitRatio):
    train = rdd.map(lambda x: (x[0], x[1][: int(len(x[1])*splitRatio)]))
    test = rdd.map(lambda x: (x[0], x[1][int(len(x[1])*splitRatio):]))
    return train,test
def splitRddV2(rdd,splitRatio):
    new_rdd = rdd.map(lambda x: (x[0], x[1][: int(len(x[1])*splitRatio)], x[1][int(len(x[1])*splitRatio):]))
    return new_rdd

#parsing data with context, but the data should be seperated into 2 different files as train and test
def parseContextData(line):
    import re
    f = lambda x : re.sub('[^0-9-.]','',x)
    line = str(line)
    uid = int(f(line.split('[')[0]))
    k = map(lambda x: x.split(','), line.split('[')[1].split('(')[1:])
    data = []
    for ele in k:
        data.append(TrainRow(int(f(ele[0])), ContextRow(int(f(ele[1])), int(f(ele[2])),
                                                       float(f(ele[3])), float(f(ele[4])),
                                                       int(f(ele[5])), int(f(ele[6])), int(f(ele[7])) ) ) )
    return (uid, data)

#parsing data with context, but the data should be seperated within the same file, same user row
def parseContextData2(line):
    import re
    f = lambda x : re.sub('[^0-9-.]','',x)
    line = str(line)
    uid = int(f(line.split('[')[0]))
    k = map(lambda x: x.split(','), line.split('[')[1].split('(')[1:])
    l = map(lambda x: x.split(','), line.split('[')[2].split('(')[1:])
    data = [[],[]]
    for ele in k:
        data[0].append(TrainRow(int(f(ele[0])), ContextRow(int(f(ele[1])), int(f(ele[2])),
                                                       float(f(ele[3])), float(f(ele[4])),
                                                       int(f(ele[5])), int(f(ele[6])), int(f(ele[7])) ) ) )
    for ele in l:
        data[1].append(TrainRow(int(f(ele[0])), ContextRow(int(f(ele[1])), int(f(ele[2])),
                                                        float(f(ele[3])), float(f(ele[4])),
                                                        int(f(ele[5])), int(f(ele[6])), int(f(ele[7])))))
    return (uid, data)

def convertTime(ts):
    hour = datetime.datetime.fromtimestamp(ts).hour
    _time_of_day = -1;
    if hour >= 5 and hour < 12:
        _time_of_day = 1 #"morning"
    if hour >= 12 and hour < 17:
        _time_of_day = 2 #"afternoon"
    if hour >= 17 and hour < 21:
        _time_of_day = 3 #"evening"
    if hour >= 21 or hour < 5:
        _time_of_day = 4 #"night"
    return _time_of_day


from bisect import bisect_left, bisect_right

class SortedCollection(object):
    '''Sequence sorted by a key function.

    SortedCollection() is much easier to work with than using bisect() directly.
    It supports key functions like those use in sorted(), min(), and max().
    The result of the key function call is saved so that keys can be searched
    efficiently.

    Instead of returning an insertion-point which can be hard to interpret, the
    five find-methods return a specific item in the sequence. They can scan for
    exact matches, the last item less-than-or-equal to a key, or the first item
    greater-than-or-equal to a key.

    Once found, an item's ordinal position can be located with the index() method.
    New items can be added with the insert() and insert_right() methods.
    Old items can be deleted with the remove() method.

    The usual sequence methods are provided to support indexing, slicing,
    length lookup, clearing, copying, forward and reverse iteration, contains
    checking, item counts, item removal, and a nice looking repr.

    Finding and indexing are O(log n) operations while iteration and insertion
    are O(n).  The initial sort is O(n log n).

    The key function is stored in the 'key' attibute for easy introspection or
    so that you can assign a new key function (triggering an automatic re-sort).

    In short, the class was designed to handle all of the common use cases for
    bisect but with a simpler API and support for key functions.
    '''

    def __init__(self, iterable=(), key=None):
        self._given_key = key
        key = (lambda x: x) if key is None else key
        decorated = sorted((key(item), item) for item in iterable)
        self._keys = [k for k, item in decorated]
        self._items = [item for k, item in decorated]
        self._key = key

    def _getkey(self):
        return self._key

    def _setkey(self, key):
        if key is not self._key:
            self.__init__(self._items, key=key)

    def _delkey(self):
        self._setkey(None)

    key = property(_getkey, _setkey, _delkey, 'key function')

    def clear(self):
        self.__init__([], self._key)

    def copy(self):
        return self.__class__(self, self._key)

    def __len__(self):
        return len(self._items)

    def __getitem__(self, i):
        return self._items[i]

    def __iter__(self):
        return iter(self._items)

    def __reversed__(self):
        return reversed(self._items)

    def __repr__(self):
        return '%s(%r, key=%s)' % (
            self.__class__.__name__,
            self._items,
            getattr(self._given_key, '__name__', repr(self._given_key))
        )

    def __reduce__(self):
        return self.__class__, (self._items, self._given_key)

    def __contains__(self, item):
        k = self._key(item)
        i = bisect_left(self._keys, k)
        j = bisect_right(self._keys, k)
        return item in self._items[i:j]

    def index(self, item):
        'Find the position of an item.  Raise ValueError if not found.'
        k = self._key(item)
        i = bisect_left(self._keys, k)
        j = bisect_right(self._keys, k)
        return self._items[i:j].index(item) + i

    def count(self, item):
        'Return number of occurrences of item'
        k = self._key(item)
        i = bisect_left(self._keys, k)
        j = bisect_right(self._keys, k)
        return self._items[i:j].count(item)

    def insert(self, item):
        'Insert a new item.  If equal keys are found, add to the left'
        k = self._key(item)
        i = bisect_left(self._keys, k)
        for el in self._items:
            if el[0] ==  item[0]:
                val = max(el[1],item[1])
                self._items[self._items.index(el)] = (el[0], val)
                return
        self._keys.insert(i, k)
        self._items.insert(i, item)
        self.remove(self[0])

    def insert_right(self, item):
        'Insert a new item.  If equal keys are found, add to the right'
        k = self._key(item)
        i = bisect_right(self._keys, k)
        self._keys.insert(i, k)
        self._items.insert(i, item)

    def remove(self, item):
        'Remove first occurence of item.  Raise ValueError if not found'
        i = self.index(item)
        del self._keys[i]
        del self._items[i]

    def find(self, k):
        'Return first item with a key == k.  Raise ValueError if not found.'
        i = bisect_left(self._keys, k)
        if i != len(self) and self._keys[i] == k:
            return self._items[i]
        raise ValueError('No item found with key equal to: %r' % (k,))

    def find_le(self, k):
        'Return last item with a key <= k.  Raise ValueError if not found.'
        i = bisect_right(self._keys, k)
        if i:
            return self._items[i-1]
        raise ValueError('No item found with key at or below: %r' % (k,))

    def find_lt(self, k):
        'Return last item with a key < k.  Raise ValueError if not found.'
        i = bisect_left(self._keys, k)
        if i:
            return self._items[i-1]
        raise ValueError('No item found with key below: %r' % (k,))

    def find_ge(self, k):
        'Return first item with a key >= equal to k.  Raise ValueError if not found'
        i = bisect_left(self._keys, k)
        if i != len(self):
            return self._items[i]
        raise ValueError('No item found with key at or above: %r' % (k,))

    def find_gt(self, k):
        'Return first item with a key > k.  Raise ValueError if not found'
        i = bisect_right(self._keys, k)
        if i != len(self):
            return self._items[i]
        raise ValueError('No item found with key above: %r' % (k,))


#usage example of SortedCollection
#sc = SortedCollection(key=lambda x:x[1])
#sc.insert((123124,0.23))
#sc.insert((123243,0.13))
#sc.insert((123245,0.35))
#print sc
#sc.remove(sc[0])
#print sc
#   output->
#SortedCollection([(123243, 0.13), (123124, 0.23), (123245, 0.35)], key=<lambda>)
#SortedCollection([(123124, 0.23), (123245, 0.35)], key=<lambda>)

