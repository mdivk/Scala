Problem Scenario 71 . 
Write down a Spark script using Python, 
In which it read a file "Content.txt" (On hdfs) with following content. 
After that split each row as (key, value), where key is first word in line and entire line as value. 
Filter out the empty lines. 
And save this key value in "problem86" as Sequence file(On hdfs) 
Part 2 : Save as sequence file , where key as null and entire line as value. Read back the stored sequence files. 
Content.txt 
Hello this is HadoopExam.com 
This is QuickTechie.com 
Apache Spark Training 
This is Spark Learning Session 
Spark is faster than MapReduce 
=========================================================================== 
Solution : 
Step 1 : 
# Import SparkContext and SparkConf 
from pyspark import SparkContext, SparkConf 
step 2 : 
#load data from hdfs 
contentRDD = sc.textFile("Content.txt") 
step 3 : 
#filter out non-empty lines 
nonempty_lines = contentRDD.filter(lambda x: len(x) > O) 
Step 4: 
#Split line based on space (Remember : It is mandatory to convert is in tuple) 
words = nonempty_lines.map(lambda x: tuple(x.split(' ',1))) 
words.saveAsSequenceFile("problem86") 
Step 5: Check contents in directory problem86 
hdfs dfs -cat problem86/part* 
Step 6 : Create key, value pair (where key is null) 
nonempty_lines.map(lambda line: (None, line)).saveAsSequenceFile("problem86_1") 
Step 7 : Reading back the sequence file data using spark. 
seqRDD = sc.sequenceFile(“problem86_1") 
Step 8 : Print the content to validate the same. 
for line in seqRDD.collect(): 
print(line) 
