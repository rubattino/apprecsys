execfile("../script/algorithms.py")
algorithms = ["MFU"]
algos = {}

algos["MFU"] = {}
algos["MFU"]["name"] = "MFU"
algos["MFU"]["function"] = "mfu_map"

metrics = {}
metrics["recall"] = {}
metrics["recall"]["name"] = "recall"
metrics["recall"]["function"] = globals()["evaluation_recall"]
metrics["averagePrecision"] = {}
metrics["averagePrecision"]["name"] = "averagePrecision"
metrics["averagePrecision"]["function"] = globals()["evaluation_ap"]
metrics["reciprocalRank"] = {}
metrics["reciprocalRank"]["name"] = "reciprocalRank"
metrics["reciprocalRank"]["function"] = globals()["evaluation_rr"]

context = {} #todo context selection

conf = {}
conf["eval"] = {}
conf["eval"]["listOfMetrics"] = ["recall","reciprocalRank","averagePrecision"]
conf["eval"]["metrics"] = metrics
conf["eval"]["listOfNs"] = [1,2,5,10]
conf["eval"]["minTestItems"] = 5
conf["eval"]["eventType"] = "opens" #installations, opens

import os
execfile("../script/utils.py")
eventsPath = os.environ["YAHOO_DATA"]
conf["eval"]["resultsFile"] = eventsPath + "/results.txt"
splitedRdd = sc.textFile(eventsPath + "/splitedData")
splitedRdd = splitedRdd.map(parseContextData2).filter(lambda row: len(row[1][1]) >= conf["eval"]["minTestItems"])


for algorithm in algorithms:
    conf["algo"] = algos[algorithm]
    algoFun = globals()[conf["algo"]["function"]]
    resultsRdd = splitedRdd.map(lambda row : algoFun(row,conf))
    results = fetchResults(resultsRdd, conf)
    writeHeader = not(os.path.isfile(conf["eval"]["resultsFile"]))
    with open(conf["eval"]["resultsFile"], "a") as myfile:
        if writeHeader:
            myfile.write("algorithm,metric,n,value\n")
        for result in results:
            myfile.write(",".join([conf["algo"]["name"],result[0],str(result[1]),str(result[2])]))
            myfile.write("\n")
 