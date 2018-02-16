Problem Scenario 70 : Write down a Spark Application using Python, 
In which it read a file "Content.txt" (On hdfs) with following content. 
Do the word count and save the results in a directory called "problem85" (On hdfs) 
Content.txt 
Hello this is Hadoop.com 
This is QuickTechie.com 
Apache Spark Training 
This is Spark Learning Session 
Spark is faster than MapReduce 

====================================================================== 
Solution : 

source file:
[paslechoix@gw03 ~]$ hdfs dfs -ls p84
Found 1 items
-rw-r--r--   3 paslechoix hdfs        143 2018-02-14 07:04 p84/Content.txt


Step 1 : Create an application with following code and store it in problem84.py 
# Import SparkContext and SparkConf 
from pyspark import SparkContext, SparkConf 
# Create configuration object and set App name 
conf = SparkConf().setAppName("CCA 175 Problem 84") 

#As after pyspark is started, sc is already available, so the next line is actually not needed
sc = SparkContext(conf=conf)  

#load data from hdfs 
contentRDD = sc.textFile("p84/Content.txt") 

>>> contentRDD.collect()
[u'Hello this is HadoopExam.com ', u'This is QuickTechie.com ', u'Apache Spark Training ', u'This is Spark Learning Session ', u'Spark is faster than MapReduce ', u'']

#filter out non-empty lines 
nonempty_lines = contentRDD.filter(lambda x: len(x) > 0)

>>> nonempty_lines.collect()
[u'Hello this is HadoopExam.com ', u'This is QuickTechie.com ', u'Apache Spark Training ', u'This is Spark Learning Session ', u'Spark is faster than MapReduce ']

#Split line based on space 
words = nonempty_lines.flatMap(lambda x: x.split(" ")) 


#Do the word count 
wordcounts = words.map(lambda x: (x, 1)) \ 
.reduceByKey(lambda x, y: x+y) \ 
.map(lambda x: (x[l],x[0])).sortByKey(Flase) 

wc = words.map(Lambda x: (x,1))

wc = words.map(Lambda x: (x,1)).reduceByKey(Lambda x, y: x+y).map(Lambda x: (x[1], x[0])).sortByKey(False)

for word in wordcounts.collect(): 
print(word) 
#Save final data
Wordcounts.saveAsTextFile(“problem84”) 
Stpe 2 : Submit this application 
spark-submit --master yarn problem84.py 



#==========================================================================================
from pyspark import SparkContext, SparkConf 
# Create configuration object and set App name 
conf = SparkConf().setAppName("CCA 175 Problem 84") 

#As after pyspark is started, sc is already available, so the next line is actually not needed
sc = SparkContext(conf=conf) 
s84RDD = sc.textFile("s84.txt") 
nonempty_lines = s84RDD.filter(lambda x: len(x) > 0)
words = nonempty_lines.flatMap(lambda x: x.split(" ")) 
wc = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[1], x[0])).sortByKey(False)
wc.saveAsTextFile("answer84")
for w in wc.collect():
	print(w)

(7, u'')
(6, u'the')
(4, u'Lorem')
(4, u'of')
(3, u'and')
(3, u'Ipsum')
(2, u'with')
(2, u'type')
(2, u'a')
(2, u'text')
(2, u'It')
(2, u'dummy')
(2, u'has')
(1, u'sheets')

wc.count()
70
wc.saveAsTextFile("solution84")

[paslechoix@gw03 ~]$ hdfs dfs -ls solution84
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-02-15 22:13 solution84/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        158 2018-02-15 22:13 solution84/part-00000
-rw-r--r--   3 paslechoix hdfs        881 2018-02-15 22:13 solution84/part-00001

[paslechoix@gw03 ~]$ hdfs dfs -cat solution84/*
(7, u'')
(6, u'the')
(4, u'Lorem')
(4, u'of')
(3, u'and')
(3, u'Ipsum')
(2, u'with')



#=============================================================================

[paslechoix@gw03 ~]$ cat p84.py
from pyspark import SparkContext, SparkConf
# Create configuration object and set App name
conf = SparkConf().setAppName("CCA 175 Problem 84")
sc = SparkContext(conf = conf)

s84RDD = sc.textFile("s84.txt")
nonempty_lines = s84RDD.filter(lambda x: len(x) > 0)
words = nonempty_lines.flatMap(lambda x: x.split(" "))
wc = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[1], x[0])).sortByKey(False)
wc.saveAsTextFile("answer84")

[paslechoix@gw03 ~]$ spark-submit --master yarn p84.py


[paslechoix@gw03 ~]$ hdfs dfs -cat answer84/*
(7, u'')
(6, u'the')
(4, u'Lorem')
(4, u'of')
(3, u'and')
(3, u'Ipsum')
(2, u'with')
