Problem Scenario 46 : You have been given belwo list in scala (name,sex,cost) for each work done. 
List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female", 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) 
Now write a Spark program to load this list as an RDD and do the sum of cost for combination of name and sex (as key) 

==================================================================================

Solution : 

Step 1 : Create an RDD out of this list 
val rdd = sc.parallelize(List( ("Deeapak" , "male",4000), ("Deepaika" , "male" ,2000), ("Deepaika" , "female" ,2000), ("Deepak" , "female" ,2000), ("Deepak" , "male" ,1000), ("Neeta" , "female" ,2000)))

step 2 : convert this RDD in pair RDD 
val byKey = rdd.map({case (name,sex,cost) => (name,sex)->cost}) 
res8: (String, String, Int) = (Deeapak,male,4000)

Step 3 : Now group by Key 
val byKeyGrouped = byKey.groupByKey 
res9: ((String, String), Iterable[Int]) = ((Deepak,female),CompactBuffer(2000))

Step 4 : Now sum the cost tor each group 
val result = byKeyGrouped.flatMapValues(v=>v)
or
val result1 = byKeyGrouped.flatMap{
   case (key, values) => values.map(v => key -> v)
}

scala> result.take(10).foreach(println)
((Deepak,female),2000)
((Deepak,male),2000)
((Deepak,male),1000)
((Deepika,female),2000)
((Deeapak,male),4000)

scala> result1.take(10).foreach(println)
((Deepak,female),2000)
((Deepak,male),2000)
((Deepak,male),1000)
((Deepika,female),2000)
((Deeapak,male),4000)

Step 5 : Save the results 

result can be saved in various number of partition(s)

result.repartition(1).saveAsTextFile("spark12/result.txt")
[paslechoix@gw03 data]$ hdfs dfs -ls spark12/result.txt
Found 2 items
-rw-r--r--   3 paslechoix hdfs          0 2018-02-28 15:23 spark12/result.txt/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        111 2018-02-28 15:23 spark12/result.txt/part-00000


result1.repartition(2).saveAsTextFile("spark12/result1.txt")
[paslechoix@gw03 data]$ hdfs dfs -ls spark12/result1.txt
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-02-28 15:23 spark12/result1.txt/_SUCCESS
-rw-r--r--   3 paslechoix hdfs         90 2018-02-28 15:23 spark12/result1.txt/part-00000
-rw-r--r--   3 paslechoix hdfs         21 2018-02-28 15:23 spark12/result1.txt/part-00001
