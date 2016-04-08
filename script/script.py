from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("mj").set("spark.executor.memory", "4g");
sc = SparkContext(conf=conf); 
execfile("/home/mert/mjrepo/apprecsys/script/context.py")
confin = parseDataRunAll("events.txt")
f = open('output.txt','w')
f.write(" ".join(str(x) for x in confin))
f.write('\n')
f.close()