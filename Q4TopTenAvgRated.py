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

def findAvg(record):
    count = 0
    sum = float("0.0")
    for val in record:
        sum = sum + val
        count = count+1
    return sum/count



readReviews = sc.textFile(reviewFile).map(lambda x: x.split("::")).map(lambda x: (x[2],float(x[3])))\
    .groupByKey()\
    .mapValues(lambda x: findAvg(x)).sortBy(lambda x: x[1], False).map(lambda x: (x[0],x[1])).take(10)

readBusiness = sc.textFile(businessFile).map(lambda x: x.split("::")).map(lambda x: (x[0],(x[1],x[2])))

out = readBusiness.join(sc.parallelize(readReviews)).map(lambda x: str(x[0])+"  "+str(x[1][0][0])+"  "+str(x[1][0][1])+"  "+str(x[1][1]))
out.repartition(1).saveAsTextFile(outputFile)


