import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
#only allowing proper words eg:"rishabh ," as 1 and "rishabh" as 1 to only "rishabh" as 1
def normalize(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("file:///Bigdata/WordCountJob/book.txt")
words = input.flatMap(normalize)

countwords = words.map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y)
#flip key and value then sort by key
countwordsSorted = countwords.map(lambda x: (x[1],x[0])).sortByKey()


results = countwordsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii','ignore')
    if (word):
        print(word.decode() + "\t" + count)
    
    

