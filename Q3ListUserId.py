from pyspark import SparkContext
from pyspark import SparkConf
import sys

argLen = len(sys.argv)
if(argLen!=4):
    print("Usage: <script_name> <business.csv input file path> <review.csv input file path> <output folder path>")
    exit(1)

businessFile = "file://" + str(sys.argv[1])
reviewFile = "file://"+ str(sys.argv[2])
outputFile = "file://"+str(sys.argv[3])

conf = SparkConf().setMaster("local").setAppName("Q3")
sc = SparkContext(conf=conf)

def countMap(record):
    count = 0
    length = len(record[1])
    if(length==2):
        l1 = set(record[1][0].split(","))
        l2= set(record[1][1].split(","))
        count = len(l1.intersection(l2))
    return count



readReviews = sc.textFile(reviewFile).map(lambda x: x.split("::"))\
    .map(lambda x: (x[2],(x[1],x[3])))
readBusiness = sc.textFile(businessFile).map(lambda x: x.split("::")).map(lambda x: (x[0],x[1]))\
    .filter(lambda x: "Stanford" in str(x[1])).map(lambda x: (x[0],x[0]))

out = readReviews.join(readBusiness).map(lambda x: x[1][0])
out.repartition(1).saveAsTextFile(outputFile)

