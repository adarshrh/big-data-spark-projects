from pyspark import SparkContext
from pyspark import SparkConf
import sys

argLen = len(sys.argv)
if(argLen!=4):
    print("Usage: <script_name> <soc-LiveJournal1Adj.txt input file path> <user data input file path> <output folder path>")
    exit(1)

friendListInputFile = "file://" + str(sys.argv[1])
userDataFile = "file://"+ str(sys.argv[2])
outputFile = "file://"+str(sys.argv[3])

conf = SparkConf().setMaster("local").setAppName("Q2")
sc = SparkContext(conf=conf)

def countMap(record):
    count = 0
    length = len(record[1])
    if(length==2):
        l1 = set(record[1][0].split(","))
        l2= set(record[1][1].split(","))
        count = len(l1.intersection(l2))
    return count

def mapToPairs(record):
    id1 = record[0]
    fdList = []
    for fid2 in record[1].split(','):
        entry = []
        if(id1 < fid2):
            pair=""+id1+","+fid2
            entry.append(pair)
            entry.append(record[1])
            fdList.append(entry)
        else:
            pair = "" + fid2 + "," + id1
            entry.append(pair)
            entry.append(record[1])
            fdList.append(entry)
    return fdList


RDDread = sc.textFile (friendListInputFile)

userInfo = sc.textFile(userDataFile)\
    .map(lambda x: x.split(','))\
    .map(lambda x: (x[0],(x[1],x[2],x[3])))
top10 = RDDread.map(lambda line: line.split('\t'))\
    .flatMap(lambda line: mapToPairs(line))\
    .groupByKey().map(lambda x: (x[0],list(x[1]))).map(lambda x: (x[0],countMap(x)))\
    .sortBy(lambda x: x[1], False).map(lambda x: (x[1],x[0])).take(10)

out = sc.parallelize(top10).map(lambda x: (x[1].split(',')[0], x)).join(userInfo)\
      .map(lambda x: ((x[1][0][1].split(',')[1]),x[1]))\
      .join(userInfo).map(lambda x: x[1])\
      .map(lambda x: (str(x[0][0][0])+"  "+str(x[0][1][0])+"  "+str(x[0][1][1]) + "  "+ str(x[0][1][2]) + "  "+ str(x[1][0])+"  "+str(x[1][1]) + "  "+ str(x[1][2])))

out.repartition(1).saveAsTextFile(outputFile)

