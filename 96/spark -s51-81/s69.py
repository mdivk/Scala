Problem Scenario 69 : Write down a Spark Application using Python, 
In which it read a file "Content.txt" (On hdfs) with following content. 
And filter out the word which is less than 2 characters and ignore all empty lines. 
Once doen store the filtered data in a directory called "problem84" (On hdfs) 
Content.txt 
Hello this is HadoopExam.com 
This is QuickTechie.com 
Apache Spark Training 
This is Spark Learning Session 
Spark is faster than MapReduce 

=========================================================================== 

Solution : 
Step 1 : Create an application with following code and store it in problem84.py 
# Import SparkContext and SparkConf 
from pyspark import SparkContext, SparkConf 
# Create configuration object and set App name 
conf = SparkConf().setAppName("CCA 175 Problem 84") 
sc = SparkContext(conf=conf) 
#load data from hdfs 
contentRDD = sc.textFile("Content.txt") 
#filter out non-empty lines 
nonempty_lines = contentRDD.filter(lambda x: len(x) > O) 
#Split line based on space 
words = nonempty_lines.flatMap(lambda x: x.split(' ')) 
#filter out all 2 letter words 
finalRDD = words.filter(lambda x: len(x) > 2) 
for word in finalRDD.collect(): 
print(word) 
#Save final data
finalRDD.saveAsTextFile(“Problem84”) 
Stpe 2 : Submit this application 
spark-submit --master yarn problem84.py 
