__author__ = 'mertergun'
# depricated way of getting context
def get_user_id(line):
    return line.split('\t')[0]


def get_item_id(line):
    return line.split('\t')[1]


def context_timestamp(line):
    return line.split('\t')[2]


def context_latitude(line):
    return float(line.split('\t')[3])


def context_longitude(line):
    return float(line.split('\t')[4])


def context_city(line):
    return line.split('\t')[5]


def context_dayOfWeek(line):
    return line.split('\t')[6]


def context_time_of_day(line):
    return line.split('\t')[7]


def context_event_type(line):
    return line.split('\t')[-1]


context = [context_timestamp, context_latitude, context_longitude, \
           context_city, context_dayOfWeek, context_time_of_day, context_event_type]

#parse data
from collections import namedtuple
import os
import datetime

execfile("../script/utils.py")
data_path = os.environ["YAHOO_DATA"]

EventDataRow = namedtuple("EventDataRow",
                          ["userId", "itemId", "ts", "latitude", "longitude", "city", "day_of_week", "time_of_day",
                           "event_type"])


def parseDataWithContext(filename):
    rawEventsRdd = sc.textFile(data_path + filename)

    def parseRawData(line):
        lineSplit = line.split("\t")
        return EventDataRow(userId=lineSplit[0],
                            itemId=lineSplit[1],
                            ts=int(lineSplit[2]),
                            latitude=float(lineSplit[3]),
                            longitude=float(lineSplit[4]),
                            city=lineSplit[5],
                            day_of_week=int(lineSplit[6]),
                            time_of_day=int(lineSplit[7]),
                            event_type=lineSplit[-1],
        )

    eventsRdd = rawEventsRdd.map(parseRawData).cache()
    userIdConversionDictionary = eventsRdd.map(lambda x: x.userId).distinct().zipWithIndex().collectAsMap()
    userIdConversionDictionaryBroadcast = sc.broadcast(userIdConversionDictionary)
    itemIdConversionDictionary = eventsRdd.map(lambda x: x.itemId).distinct().zipWithIndex().collectAsMap()
    itemIdConversionDictionaryBroadcast = sc.broadcast(itemIdConversionDictionary)
    cityConversionDictionary = eventsRdd.map(lambda x: x.city).distinct().zipWithIndex().collectAsMap()
    cityConversionDictionaryBroadcast = sc.broadcast(cityConversionDictionary)

    eventsConvertedRdd = eventsRdd.map(lambda x: EventDataRow(
        userId=userIdConversionDictionaryBroadcast.value[x.userId],
        itemId=itemIdConversionDictionaryBroadcast.value[x.itemId],
        ts=x.ts,
        latitude=x.latitude,
        longitude=x.longitude,
        city=cityConversionDictionaryBroadcast.value[x.city],
        day_of_week=x.day_of_week,
        time_of_day=x.time_of_day,
        event_type=x.event_type
    ))
    finalRDD = eventsConvertedRdd.map(lambda x: [
        x.userId, (
            x.itemId,
            x.ts,
            x.latitude,
            x.longitude,)
    ])

    groupData = map((lambda (x, y): (x, sorted(list(y), key=lambda a: a[1]))), sorted(finalRDD.groupByKey().collect()))

    def detectMovement(x):
        data = x[1]
        newData = [(data[0][0], data[0][1], data[0][2], data[0][3], 1)]
        for i in xrange(1, len(data)):
            event = data[i]
            distance = haversine(event[3], event[2], data[i - 1][3], data[i - 1][2]) * 1000  #in meters
            time_difference = event[1] - newData[i - 1][1]  #in seconds
            moving = 1  #not available
            if time_difference <= 300:  #if 2 consecutive events are more than 300 seconds away, the movement is not available
                velocity = distance / time_difference if time_difference > 0 else -1
                if velocity < 0:
                    moving = 1;  #not available
                elif velocity >= 0 and velocity <= 1:
                    moving = 2  #standing still
                elif velocity <= 2.4:
                    moving = 3  #walking spead
                else:
                    moving = 4  #faster
            newData.append((event[0], event[1], event[2], event[3], moving))
        return (x[0], newData)

    groupData = sc.parallelize(groupData).map(detectMovement).cache()

    def convertLocation(line):
        listGroup = line[1]
        workGroup = [x for x in listGroup if datetime.datetime.fromtimestamp(int(x[1])).hour >= 6 and
                     datetime.datetime.fromtimestamp(int(x[1])).hour <= 18]
        numNearLocation = []
        i = 0
        for x in workGroup:
            numNearLocation.append(0);
            for y in workGroup:
                if haversine(x[3], x[2], y[3], y[2]) < 0.1:
                    numNearLocation[i] = numNearLocation[i] + 1
            i = i + 1
        index_work = numNearLocation.index(max(numNearLocation))
        #     workGroup = [(x[0],x[1],x[2],x[3],1) if haversine(x[3], x[2], workGroup[index_work][3], workGroup[index_work][2]) < 0.1
        #                  else (x[0],x[1],x[2],x[3],0) for x in workGroup]
        #workGroup3 = [(x[0],x[1],0)  for x in workGroup if haversine(x[3], x[2], workGroup[index][3], workGroup[index][2]) >= 0.1]

        homeGroup = [x for x in listGroup if datetime.datetime.fromtimestamp(int(x[1])).hour < 6 or
                     datetime.datetime.fromtimestamp(int(x[1])).hour > 18]

        numNearLocation = []
        i = 0
        for x in homeGroup:
            numNearLocation.append(0);
            for y in homeGroup:
                if haversine(x[3], x[2], y[3], y[2]) < 0.1:
                    numNearLocation[i] = numNearLocation[i] + 1
            i = i + 1
        index_home = numNearLocation.index(max(numNearLocation))
        #     homeGroup = [(x[0],x[1],x[2],x[3],2) if haversine(x[3], x[2], homeGroup[index_home][3], homeGroup[index_home][2]) < 0.1
        #                  else (x[0],x[1],x[2],x[3],0) for x in homeGroup]

        listGroup = [
            (x[0], x[1], x[4], 1) if haversine(x[3], x[2], workGroup[index_work][3], workGroup[index_work][2]) < 0.01
            else(
                (x[0], x[1], x[4], 2) if haversine(x[3], x[2], homeGroup[index_home][3],
                                                   homeGroup[index_home][2]) < 0.01
                else (x[0], x[1], x[4], 3)
            )
            for x in listGroup]

        listGroup = [(x[0], x[2], x[3], 1) if datetime.datetime.fromtimestamp(int(x[1])).hour >= 8 and
                                              datetime.datetime.fromtimestamp(int(x[1])).hour <= 13
                     else(
            (x[0], x[2], x[3], 2) if datetime.datetime.fromtimestamp(int(x[1])).hour >= 13 and
                                     datetime.datetime.fromtimestamp(int(x[1])).hour <= 18
            else (x[0], x[2], x[3], 3)
        )
                     for x in listGroup]


        #time = datetime.datetime.fromtimestamp(int(line[1][0][1]))
        #line[1][1] = datetime.datetime.fromtimestamp(int("1284101485")).strftime('%Y-%m-%d %H:%M:%S')
        #return line[0],(workGroup+homeGroup)[:20],listGroup[:20]#,len(workGroup+homeGroup),len(workGroup),len(homeGroup)
        return line[0], listGroup

    return groupData.map(convertLocation)


def parseDataRunAll(filename):
    rawEventsRdd = sc.textFile(data_path + filename)

    def parseRawData(line):
        lineSplit = line.split("\t")
        return EventDataRow(userId=lineSplit[0],
                            itemId=lineSplit[1],
                            ts=int(lineSplit[2]),
                            latitude=float(lineSplit[3]),
                            longitude=float(lineSplit[4]),
                            city=lineSplit[5],
                            day_of_week=int(lineSplit[6]),
                            time_of_day=int(lineSplit[7]),
                            event_type=lineSplit[-1],
        )

    eventsRdd = rawEventsRdd.map(parseRawData).cache()
    userIdConversionDictionary = eventsRdd.map(lambda x: x.userId).distinct().zipWithIndex().collectAsMap()
    userIdConversionDictionaryBroadcast = sc.broadcast(userIdConversionDictionary)
    itemIdConversionDictionary = eventsRdd.map(lambda x: x.itemId).distinct().zipWithIndex().collectAsMap()
    itemIdConversionDictionaryBroadcast = sc.broadcast(itemIdConversionDictionary)
    cityConversionDictionary = eventsRdd.map(lambda x: x.city).distinct().zipWithIndex().collectAsMap()
    cityConversionDictionaryBroadcast = sc.broadcast(cityConversionDictionary)

    eventsConvertedRdd = eventsRdd.map(lambda x: EventDataRow(
        userId=userIdConversionDictionaryBroadcast.value[x.userId],
        itemId=itemIdConversionDictionaryBroadcast.value[x.itemId],
        ts=x.ts,
        latitude=x.latitude,
        longitude=x.longitude,
        city=cityConversionDictionaryBroadcast.value[x.city],
        day_of_week=x.day_of_week,
        time_of_day=x.time_of_day,
        event_type=x.event_type
    ))
    finalRDD = eventsConvertedRdd.map(lambda x: [
        x.userId, (
            x.itemId,
            x.ts)
    ])
    groupData = finalRDD.groupByKey().map(lambda (x, y): (x, list(y)))
    def remove_duplicates(values):
        output = []
        seen = set()
        for value in values:
            # If value has not been encountered yet,
            # ... add it to both list and set.
            if value not in seen:
                output.append(value)
                seen.add(value)
        return output

    def lruFunction(line):
        listGroup = line[1]
        #shuffle(listGroup)                  #shuffle the list
        #listGroup = sorted(listGroup,key=lambda x:int(x[1]), reverse=True);
        listGroup = sorted(listGroup,key=lambda x:int(x[1]));
        l = len(listGroup)
        numTrain = l * 8 / 10
        numTest = l - numTrain
        trainList = listGroup[:numTrain]      #0.8 train set
        testList = listGroup[numTrain:]       #0.2 test set

        trainList = sorted(trainList,key=lambda x:int(x[1]), reverse=True);  #sort by timestamp with descending
        testList = [t[0] for t in testList]                      #take only id for test set
        RecommenderDuplicate =  [t[0] for t in trainList]        #take only id for train set
        Recommender = remove_duplicates(RecommenderDuplicate)    #remove duplicate

        if len(Recommender) > 4:
            finalRecommender = Recommender[:5]
        else:
            finalRecommender = [-1,-1,-1,-1,-1]
            numRec = len(finalRecommender)
            finalRecommender[:numRec] = Recommender

        scores = 0
        numHit = 0
        for t in testList:
            if t == finalRecommender[0]:
                scores = scores+1
                numHit = numHit+1
            elif t == finalRecommender[1]:
                scores = scores+0.8
                numHit = numHit+1
            elif t == finalRecommender[2]:
                scores = scores+0.6
                numHit = numHit+1
            elif t == finalRecommender[3]:
                scores = scores+0.4
                numHit = numHit+1
            elif t == finalRecommender[4]:
                scores = scores+0.2
                numHit = numHit+1
            else:
                numHit = numHit+1

        #scores = scores / numTest
        scores = scores / numHit
        return scores
    finalScore_lru = groupData.map(lruFunction).mean()
    def mruFunction(line):
        listGroup = line[1]
        listGroup = sorted(listGroup,key=lambda x:int(x[1]));
        l = len(listGroup)
        numTrain = l * 8 / 10
        numTest = l - numTrain
        trainList = listGroup[:numTrain]      #0.9 train set
        testList = listGroup[numTrain:]       #0.1 test set

        testList = [t[0] for t in testList]                      #take only id for test set
        RecommenderDuplicate =  [t[0] for t in trainList]        #take only id for train set
        Recommender = remove_duplicates(RecommenderDuplicate)    #remove duplicate

        if len(Recommender) > 4:
            finalRecommender = Recommender[:5]
        else:
            finalRecommender = [-1,-1,-1,-1,-1]
            numRec = len(finalRecommender)
            finalRecommender[:numRec] = Recommender

        scores = 0
        numHit = 0
        for t in testList:
            if t == finalRecommender[0]:
                scores = scores+1
                numHit = numHit+1
            elif t == finalRecommender[1]:
                scores = scores+0.8
                numHit = numHit+1
            elif t == finalRecommender[2]:
                scores = scores+0.6
                numHit = numHit+1
            elif t == finalRecommender[3]:
                scores = scores+0.4
                numHit = numHit+1
            elif t == finalRecommender[4]:
                scores = scores+0.2
                numHit = numHit+1
            else:
                numHit = numHit+1

        #scores = scores / numTest
        scores = scores / numHit
        return scores
    finalScore_mru = groupData.map(mruFunction).mean()
    from collections import Counter
    def mfuFunction(line):
        listGroup = line[1]
        #shuffle(listGroup)                  #shuffle the list
        #listGroup = sorted(listGroup,key=lambda x:int(x[1]), reverse=True);
        listGroup = sorted(listGroup,key=lambda x:int(x[1]));
        l = len(listGroup)
        numTrain = l * 8 / 10
        numTest = l - numTrain
        trainList = listGroup[:numTrain]      #0.9 train set
        testList = listGroup[numTrain:]       #0.1 test set

        testList = [t[0] for t in testList]                      #take only id for test set
        RecommenderDuplicate =  [t[0] for t in trainList]        #take only id for train set
        #Recommender = remove_duplicates(RecommenderDuplicate)    #remove duplicate
        Recommender = Counter(RecommenderDuplicate).most_common()
        Recommender =  [t[0] for t in Recommender]

        if len(Recommender) > 4:
            finalRecommender = Recommender[:5]
        else:
            finalRecommender = [-1,-1,-1,-1,-1]
            numRec = len(finalRecommender)
            finalRecommender[:numRec] = Recommender

        scores = 0
        numHit = 0
        for t in testList:
            if t == finalRecommender[0]:
                scores = scores+1
                numHit = numHit+1
            elif t == finalRecommender[1]:
                scores = scores+0.8
                numHit = numHit+1
            elif t == finalRecommender[2]:
                scores = scores+0.6
                numHit = numHit+1
            elif t == finalRecommender[3]:
                scores = scores+0.4
                numHit = numHit+1
            elif t == finalRecommender[4]:
                scores = scores+0.2
                numHit = numHit+1
            else:
                numHit = numHit+1

        #scores = scores / numTest
        scores = scores / numHit
        return scores
        #return Recommender
        #return testList[0],testList[1],testList[2],testList[3],testList[4]
        #if len(Recommender) > 4:
        #    return line[0], Recommender[0], Recommender[1], Recommender[2], Recommender[3], Recommender[4]
        #return line[0],trainList[0],trainList[1],trainList[2],trainList[3],trainList[4],trainList[5],trainList[6],trainList[7]
        #return l,len(trainList),len(testList)
    finalScore_mfu = groupData.map(mfuFunction).mean()

    #BAYESIAN
    finalRDD = eventsConvertedRdd.map(lambda x: [
        x.userId, (
            x.itemId,
            x.ts,
            x.latitude,
            x.longitude,)
    ])
    groupData = finalRDD.groupByKey().map(lambda (x,y): (x, sorted(list(y),key=lambda a: a[1])))
    def detectMovement(x):
        data = x[1]
        newData = [(data[0][0], data[0][1], data[0][2], data[0][3], 1)]
        for i in xrange(1,len(data)):
            event = data[i]
            distance = haversine(event[3],event[2], data[i-1][3], data[i-1][2]) * 1000 #in meters
            time_difference = event[1] - newData[i-1][1] #in seconds
            moving = 1 #not available
            if time_difference <= 300: #if 2 consecutive events are more than 300 seconds away, the movement is not available
                velocity =  distance/time_difference if time_difference > 0 else -1
                if velocity < 0:
                    moving = 1; #not available
                elif velocity >= 0 and velocity <= 1:
                    moving = 2  #standing still
                elif velocity <=2.4:
                    moving = 3 #walking spead
                else:
                    moving = 4 #faster
            newData.append((event[0],event[1],event[2],event[3], moving))
        return (x[0], newData)
    groupData = groupData.map(detectMovement).cache()

    def convertLocation(line):
        listGroup = line[1]
        workGroup = [x for x in listGroup if datetime.datetime.fromtimestamp(int(x[1])).hour >= 6 and
                datetime.datetime.fromtimestamp(int(x[1])).hour <= 18]
        numNearLocation = []
        i = 0
        for x in workGroup:
            numNearLocation.append(0);
            for y in workGroup:
                if haversine(x[3], x[2], y[3], y[2]) < 0.1:
                    numNearLocation[i] = numNearLocation[i] + 1
            i = i + 1
        index_work = numNearLocation.index(max(numNearLocation))
    #     workGroup = [(x[0],x[1],x[2],x[3],1) if haversine(x[3], x[2], workGroup[index_work][3], workGroup[index_work][2]) < 0.1
    #                  else (x[0],x[1],x[2],x[3],0) for x in workGroup]
        #workGroup3 = [(x[0],x[1],0)  for x in workGroup if haversine(x[3], x[2], workGroup[index][3], workGroup[index][2]) >= 0.1]

        homeGroup = [x for x in listGroup if datetime.datetime.fromtimestamp(int(x[1])).hour < 6 or
                datetime.datetime.fromtimestamp(int(x[1])).hour > 18]

        numNearLocation = []
        i = 0
        for x in homeGroup:
            numNearLocation.append(0);
            for y in homeGroup:
                if haversine(x[3], x[2], y[3], y[2]) < 0.1:
                    numNearLocation[i] = numNearLocation[i] + 1
            i = i + 1
        index_home = numNearLocation.index(max(numNearLocation))
    #     homeGroup = [(x[0],x[1],x[2],x[3],2) if haversine(x[3], x[2], homeGroup[index_home][3], homeGroup[index_home][2]) < 0.1
    #                  else (x[0],x[1],x[2],x[3],0) for x in homeGroup]

        listGroup = [(x[0],x[1],x[4],1) if haversine(x[3], x[2], workGroup[index_work][3], workGroup[index_work][2]) < 0.01
                     else(
                        (x[0],x[1],x[4],2) if haversine(x[3], x[2], homeGroup[index_home][3], homeGroup[index_home][2]) < 0.01
                        else (x[0],x[1],x[4],3)
                        )
                     for x in listGroup]

        listGroup = [(x[0],x[2],x[3],1) if datetime.datetime.fromtimestamp(int(x[1])).hour >= 8 and
                    datetime.datetime.fromtimestamp(int(x[1])).hour <= 13
                        else(
                          (x[0],x[2],x[3],2) if datetime.datetime.fromtimestamp(int(x[1])).hour >= 13 and
                            datetime.datetime.fromtimestamp(int(x[1])).hour <= 18
                          else (x[0],x[2],x[3],3)
                        )
                    for x in listGroup]


        #time = datetime.datetime.fromtimestamp(int(line[1][0][1]))
        #line[1][1] = datetime.datetime.fromtimestamp(int("1284101485")).strftime('%Y-%m-%d %H:%M:%S')
        #return line[0],(workGroup+homeGroup)[:20],listGroup[:20]#,len(workGroup+homeGroup),len(workGroup),len(homeGroup)
        return line[0],listGroup
    final = groupData.map(convertLocation)

    def remove_duplicates(values):
        output = []
        seen = set()
        for value in values:
            # If value has not been encountered yet,
            # ... add it to both list and set.
            if value not in seen:
                output.append(value)
                seen.add(value)
        return output

    def bayesian(line):
        listGroup = line[1]
        #shuffle(listGroup)                  #shuffle the list
        l = len(listGroup)
        numTrain = l * 8 / 10
        numTest = l - numTrain
        trainList = listGroup[:numTrain]      #0.8 train set
        testList = listGroup[numTrain:]       #0.2 test set

        #trainRDD = sc.parallelize(trainList).count()
        newTestList = []
        for t in testList:
            context = [x for x in trainList if x[1]==t[1] and x[2]==t[2] and x[3]==t[3]]
            numContext = float(len(context))
            p_context = numContext/numTrain  #P(C1i, C2j, C3k)
            p_app = []
            context_no_duplicate = remove_duplicates(context)
            for c in context_no_duplicate:
                appi = [x for x in trainList if x[0]==c[0]]
                numAppi = float(len(appi))
                p_appi = numAppi/numTrain
                contextAppi = [x for x in trainList if x[0]==c[0] and x[1]==c[1] and x[2]==c[2] and x[3]==c[3]]
                if numAppi != 0:    #P(C1i, C2j, C3k | APPid)
                    p_contextAppi = len(contextAppi)/numAppi
                else:
                    p_contextAppi = 0
                if p_context != 0:  #P(APPid | C1i,C2j,C3k = P(C1i, C2j, C3k | APPid)  P(APPid) /P(C1i, C2j, C3k)
                    p = p_contextAppi * p_appi / p_context
                else:
                    p = 0
                p_app.append((c[0],p))
            app_sort = sorted(p_app,key=lambda x: -x[1])
            if len(app_sort)>=5:
                app_rec = map(lambda x:x[0],app_sort[:5])
            else:
                app_rec = [-1,-1,-1,-1,-1]
                app_rec[:len(app_sort)] = map(lambda x:x[0],app_sort)
            newTestList.append((t[0],app_rec))

        scores = 0
        numHit = 0
        for t in newTestList:
            if t[0] == t[1][0]:
                scores = scores+1
                numHit = numHit+1
            elif t[0] == t[1][1]:
                scores = scores+0.8
                numHit = numHit+1
            elif t[0] == t[1][2]:
                scores = scores+0.6
                numHit = numHit+1
            elif t[0] == t[1][3]:
                scores = scores+0.4
                numHit = numHit+1
            elif t[0] == t[1][4]:
                scores = scores+0.2
                numHit = numHit+1
            else:
                numHit = numHit+1

        #scores = scores / numTest
        scores = scores / numHit

        #return newTestList[:20]
        return scores
    result_bay = final.map(bayesian).mean()
    return finalScore_lru, finalScore_mru, finalScore_mfu, result_bay
