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

Data preparation:

[paslechoix@gw03 ~]$ cat p71.txt
Hello this is HadoopExam.com

This is QuickTechie.com
Apache Spark Training

This is Spark Learning Session
Spark is faster than MapReduce
[paslechoix@gw03 ~]$ hdfs dfs -put p71.txt 

[paslechoix@gw03 ~]$ hdfs dfs -cat p71.txt
Hello this is HadoopExam.com

This is QuickTechie.com
Apache Spark Training

This is Spark Learning Session
Spark is faster than MapReduce
[paslechoix@gw03 ~]$


Step 1 : 
# Import SparkContext and SparkConf 
from pyspark import SparkContext, SparkConf 
step 2 : 
#load data from hdfs 
p71RDD = sc.textFile("p71.txt") 
step 3 : 

>>> for line in nonempty_lines.collect(): print(line)

#filter out non-empty lines 
nonempty_lines = p71RDD.filter(lambda x: len(x) > O) 
Hello this is HadoopExam.com
This is QuickTechie.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce

Step 4: 
#Split line based on space (Remember : It is mandatory to convert is in tuple) 

words = nonempty_lines.map(lambda x: tuple(x.split(' ',1))) 

for w in words.collect(): print(w)
(u'Hello', u'this is HadoopExam.com ')
(u'This', u'is QuickTechie.com ')
(u'Apache', u'Spark Training ')
(u'This', u'is Spark Learning Session ')
(u'Spark', u'is faster than MapReduce ')

words.saveAsSequenceFile("problem86") 

Step 5: Check contents in directory problem86 
[paslechoix@gw03 ~]$ hdfs dfs -cat problem86_seq/*
SEQorg.apache.hadoop.io.Textorg.apache.hadoop.io.Text▒^▒T▒w▒kl|%▒}O+Hellothis is HadoopExam.com Thisis QuickTechie.com ApacheSpark Training SEQorg.apache.hadoop.io.Textorg.apache.hadoop.io.Text͏oBf▒▒P▒=▒▒▒ Thisis Spark Learning Session  Sparkis faster than MapReduce [paslechoix@gw03 ~]$


Step 6 : Reading back the sequence file data using spark. 
seqRDD = sc.sequenceFile(problem86_seq") 
(u'Hello', u'this is HadoopExam.com ')
(u'This', u'is QuickTechie.com ')
(u'Apache', u'Spark Training ')
(u'This', u'is Spark Learning Session ')
(u'Spark', u'is faster than MapReduce ')

