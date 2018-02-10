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

val contentRDD = sc.textFile("spark2/Content.txt")

scala> contentRDD.take(10).foreach(println)
Hello this is HadoopExam.com
This is QuickTechie.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce

val contentRDDMap = contentRDD.map(rec => (rec.split(" ")(0), rec))
scala> contentRDDMap.take(10).foreach(println)
(Hello,Hello this is HadoopExam.com )
(This,This is QuickTechie.com )
(Apache,Apache Spark Training )
(This,This is Spark Learning Session )
(Spark,Spark is faster than MapReduce )

Save as sequence file , where key as null and entire line as value. Read back the stored sequence files.

contentRDDMap.saveAsSequenceFile("problem86")

[paslechoix@gw01 data]$ hdfs dfs -ls problem86
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-02-09 20:24 problem86/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        198 2018-02-09 20:24 problem86/part-00000
-rw-r--r--   3 paslechoix hdfs        169 2018-02-09 20:24 problem86/part-00001

ContentWithBlankLines.txt

Hello this is HadoopExam.com 
This is QuickTechie.com 

Apache Spark Training 
This is Spark Learning Session 

Spark is faster than MapReduce 

[paslechoix@gw01 data]$ vim ContentWithBlankLines.txt
[paslechoix@gw01 data]$ hdfs dfs -put ContentWithBlankLines.txt ContentWithBlankLines.txt
[paslechoix@gw01 data]$ hdfs dfs -ls ContentWithBlankLines.txt
-rw-r--r--   3 paslechoix hdfs        144 2018-02-09 20:33 ContentWithBlankLines.txt
[paslechoix@gw01 data]$ hdfs dfs -cat ContentWithBlankLines.txt

Hello this is HadoopExam.com
This is QuickTechie.com

Apache Spark Training
This is Spark Learning Session

Spark is faster than MapReduce
[paslechoix@gw01 data]$

scala> contentWithBlankLinesRDD.collect
res5: Array[String] = Array("", "Hello this is HadoopExam.com ", "This is QuickTechie.com ", "", "Apache Spark Training ", "This is Spark Learning Session ", "", Spark is faster than MapReduce)

scala> val contentWithBlankLinesRDDMap = contentWithBlankLinesRDD.map(rec =>(rec.split(" ")(0), rec))

scala> contentWithBlankLinesRDDMap.take(10).foreach(println)
(,)
(Hello,Hello this is HadoopExam.com )
(This,This is QuickTechie.com )
(,)
(Apache,Apache Spark Training )
(This,This is Spark Learning Session )
(,)
(Spark,Spark is faster than MapReduce)

scala> val contentFilteredRDD = contentWithBlankLinesRDDMap.filter(rec => rec._1.length>0)

(Hello,Hello this is HadoopExam.com )
(This,This is QuickTechie.com )
(Apache,Apache Spark Training )
(This,This is Spark Learning Session )
(Spark,Spark is faster than MapReduce)

