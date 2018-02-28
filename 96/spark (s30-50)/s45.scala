Problem Scenario 45 : You have been given 2 files , with the content as given in RHS. 
(spark12/technology.txt) 

joe,doe,hadoop
jane,doe,spark
john,smith,hortonworks
jennifer,smith,cloudera

(spark12/salary.txt) 
joe,doe,100
jane,doe,200
john,smith,300
jennifer,smith,400

Write a Spark program, which will join the data based on first and last name and save the joined results in following format. 

first,last,technology,salary 
==================================================================================

Solution : 

Step 1 : Create 2 files first using Hue in hdfs. 

step 2 : Load all file as an RDD 

val tech = sc.textFile("spark12/technology.txt").map(e => e.split(",")) 
res0: Array[String] = Array(joe, doe, hadoop)

val salary = sc.textFile("spark12/salary.txt").map(e => e.split(",")) 
res1: Array[String] = Array(joe, doe, 100)

Step 3 : Now create Key,value pair ot data and join them. 
val joined = tech.map(e=>((e(0),e(1)),e(2))).join(salary.map(e=>((e(0),e(1)),e(2))))
res2: ((String, String), (String, String)) = ((john,smith),(hortonworks,300))

val result = joined.map(x=>(x._1, x._2._1, x._2._2))
res4: ((String, String), String, String) = ((john,smith),hortonworks,300)

Step 4 : Save the results in a text file as below. 
result.repartition(1).saveAsTextFile("spark12/multiColumnJoined.txt") 

[paslechoix@gw03 data]$ hdfs dfs -cat spark12/multiColumnJoined.txt/*
((john,smith),hortonworks,300)
((jennifer,smith),cloudera,400)
((joe,doe),hadoop,100)
((jane,doe),spark,200)
