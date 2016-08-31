__author__ = 'mertergun'
from math import radians, cos, sin, asin, sqrt
EventRow = namedtuple("event", ["userId", "itemId","ts","city","lat","lon"])
EventInstallRow = namedtuple("event", ["userId", "itemId","ts","city","lat","lon","event_type"])
TrainRow = namedtuple("train", ["itemId", "context"])
TrainInstallRow = namedtuple("train", ["itemId", "context","event_type"])
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

def loadInstalledDataset(path):
    events = sc.textFile(path).map(lambda line: line[1:-1].split(",")).map(lambda x: EventInstallRow(
        userId=int(x[0]),
        itemId=int(x[1]),
        ts=int(x[2]),
        city=int(x[3]),
        lat=float(x[6]),
        lon=float(x[7]),
        event_type=int(x[8])
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

def splitRddV2install(rdd,splitRatio):
    new_rdd = rdd.map(lambda x: (x[0], x[1][: int(len(x[1])*splitRatio)], x[1][int(len(x[1])*splitRatio):], x[2]))
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

def parseContextData2install(line):
    data = eval(line)
    uid = data[0]
    parsedData = [[],[], []]
    for ele in data[1]:
        parsedData[0].append(TrainInstallRow(ele[0], ContextRow(*list(ele)[1:8]), ele[8]))
    for ele in data[2]:
        parsedData[1].append(TrainInstallRow(ele[0], ContextRow(*list(ele)[1:8]), ele[8]))
    for ele in data[3]:
        parsedData[2].append(TrainInstallRow(ele[0], ContextRow(*list(ele)[1:8]), ele[8]))
    return (uid, parsedData)

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


            
        