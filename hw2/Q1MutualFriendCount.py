from pyspark import SparkContext
from pyspark import SparkConf
import sys

argLen = len(sys.argv)
if(argLen!=3):
    print()
    print("Usage: <script_name> <soc-LiveJournal1Adj.txt input file path> <output folder path>")
    print()
    exit(1)

inputFile = "file://"+str(sys.argv[1])
outputFile = "file://"+str(sys.argv[2])

conf = SparkConf().setMaster("local").setAppName("Q1")
sc = SparkContext(conf=conf)

def countMap(record):
    count = 0
    length = len(record[1])
    ans = ""
    if(length==2):
        l1 = set(record[1][0].split(","))
        l2= set(record[1][1].split(","))
        count = len(l1.intersection(l2))
    ans = ans+record[0]+"   "+str(count)
    return ans

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


RDDread = sc.textFile (inputFile)

RDDread.map(lambda line: line.split('\t'))\
    .flatMap(lambda line: mapToPairs(line))\
    .groupByKey().map(lambda x: (x[0],list(x[1]))).map(lambda x: countMap(x))\
    .saveAsTextFile(outputFile)