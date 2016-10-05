from collections import Counter


def evaluation_rr(testList, recList):
    scores = 0
    for t in testList:
        for i in range(len(recList)):
            if t == recList[i]:
                scores = scores + 1.0 / (i+1)
                break
    scores = scores / len(testList)
    return scores

def evaluation_ap(testList, recList):
    scores = 0
    num = 0 
    rr = []
    crr = []
    rr[:len(recList)] = [0 for i in range(len(recList))]
    for t in testList:
        for i in range(len(recList)):
            if t == recList[i]:
                rr[i] = 1
    
    for t in testList:    
        for i in range(len(recList)):
            if t == recList[i]:
                for j in range(i,len(recList)):    
                    if rr[j] == 1:
                        scores = scores + 1.0 / (j+1)
                        num = num + 1
                break
    if num == 0:
        scores = 0
    else:
        scores = scores / num
    return scores

def evaluation_recall(testList, recList):
    scores = 0
    num = 0
    recall = []
    recall[:len(recList)] = [0 for i in range(len(recList))]
    rr=[]
    rr[:len(recList)] = [0 for i in range(len(recList))]
    for t in testList:
        for i in range(len(recList)):
            if t == recList[i]:
                rr[i] = 1
    for t in testList:    
        for i in range(len(recList)):
            if t == recList[i]:
                for j in range(i,len(recList)):    
                    recall[j] = recall[j] + 1.0
                    num = num + 1  
                break    
    recall = [0 if num == 0 else x*sum(rr)/num for x in recall]
    return recall

def evaluationWithN(testList, recList, n, conf):
    results = []
    for metric in conf["eval"]["listOfMetrics"]:
        metricFun = conf["eval"]["metrics"][metric]["function"]
        results.append(metricFun(testList, recList))
    return tuple(results)


## MFU
def mfu_map(itemList,conf):
    listGroup = itemList[1]
    l = len(listGroup)
    trainList = listGroup[0]  # 0.8 train set
    testList = listGroup[1]  # 0.2 test set
    numTrain = len(trainList)
    numTest = l - numTrain

    testList = [t[0] for t in testList]  # take only itemid for test set
    RecommenderDuplicate = [t[0] for t in trainList]  # take only itemid for train set
    # Recommender = remove_duplicates(RecommenderDuplicate)    #remove duplicate
    Recommender = Counter(RecommenderDuplicate).most_common()
    Recommender = [t[0] for t in Recommender]
    results = []
    for n in conf["eval"]["listOfNs"]:
        if len(Recommender) > n-1:
            finalRecommender = Recommender[:n]
        else:
            finalRecommender = [-1] * n
            numRec = len(Recommender)
            finalRecommender[:numRec] = Recommender
        #scores_rr = evaluation_rr(testList, finalRecommender)
        #scores_ap = evaluation_ap(testList, finalRecommender)
        #scores_recall = evaluation_recall(testList, finalRecommender)
        #results.append((scores_rr, scores_ap, scores_recall))
        results.append(evaluationWithN(testList, finalRecommender, n, conf))
    return results
        
