__author__ = 'mertergun'
from math import radians, cos, sin, asin, sqrt
from collections import namedtuple
import datetime
EventRow = namedtuple("event", ["userId", "itemId","ts","city","lat","lon"])
TrainRow = namedtuple("train", ["itemId", "context"])
ContextRow = namedtuple("context", ["ts","city", "lat", "lon", "moving", "location" ])

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

def splitRdd(rdd,splitRatio):
    train = rdd.map(lambda x: (x[0], x[1][: int(len(x[1])*splitRatio)]))
    test = rdd.map(lambda x: (x[0], x[1][int(len(x[1])*splitRatio):]))
    return train,test
def splitRddV2(rdd,splitRatio):
    new_rdd = rdd.map(lambda x: (x[0], x[1][: int(len(x[1])*splitRatio)], x[1][int(len(x[1])*splitRatio):]))
    return new_rdd

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
                                                       int(f(ele[5])), int(f(ele[6])) ) ) )
    return (uid, data)

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
                                                       int(f(ele[5])), int(f(ele[6])) ) ) )
    for ele in l:
        data[1].append(TrainRow(int(f(ele[0])), ContextRow(int(f(ele[1])), int(f(ele[2])),
                                                        float(f(ele[3])), float(f(ele[4])),
                                                        int(f(ele[5])), int(f(ele[6])))))
    return (uid, data)

#notfinished
class Sorted_List:
    def __init__(self, maxsize, compare_func):
        self.lst = []
        self.size = maxsize
        self.func = compare_func # f(x,y) returns true if x > y else false
    
    def add(self,el):
        if len(self.lst) < self.size:
            #binary sort-insert
            i_begin = 0
            i_end = len(self.lst)
            while i_begin < i_end:
                i = (i_begin + i_end)/2
                if self.func(self.lst[i],el):
                    i_begin = i + 1
                else:
                    i_end = i - 1
            self.lst.insert(i_begin, el)
            del(self.lst[-1])
            
            
        