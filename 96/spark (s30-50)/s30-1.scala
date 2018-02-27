Problem Scenario 30 : You have been given three csv files in hdfs as below. 
EmployeeName.csv with the field (id, name) 
EmployeeManager.csv (id, managerName) 
Employeesalary.csv (id, Salary) 
using Spark and its API you have to generate a joined output as below and save as a text file (Separated by comma) tor final distribution and output must be sorted by id.
Id,name,salary,managerName 
==================================================================================
Solution : 

Step 1 : Create all three files in hdfs in directory called spark1(We will do using Hue). However, you can first create in local filesystem and then upload it to hdfs.
Step 2 : Load EmployeeManager.csv file from hdfs and create PairRDDs 

vim mgr1.csv
1, Cliff
2, Raj
3, Alim
4, Jose
5, Jeff

vim sal1.csv
1,100
2,200
3,300
4,400
5,500


[paslechoix@gw03 ~]$ hdfs dfs -put mgr1.csv spark1/
[paslechoix@gw03 ~]$ hdfs dfs -ls spark1
Found 9 items
-rw-r--r--   3 paslechoix hdfs        100 2018-02-04 17:22 spark1/EmployeeManager.csv
-rw-r--r--   3 paslechoix hdfs         82 2018-02-04 17:21 spark1/EmployeeName.csv
-rw-r--r--   3 paslechoix hdfs         46 2018-02-04 17:22 spark1/Employeesalary
-rw-r--r--   3 paslechoix hdfs         70 2018-02-25 22:00 spark1/mgr.csv
-rw-r--r--   3 paslechoix hdfs         40 2018-02-26 21:17 spark1/mgr1.csv
drwxr-xr-x   - paslechoix hdfs          0 2018-02-04 17:51 spark1/result.txt
-rw-r--r--   3 paslechoix hdfs         36 2018-02-25 22:13 spark1/sal.csv
-rw-r--r--   3 paslechoix hdfs         31 2018-02-26 21:17 spark1/sal1.csv
-rw-r--r--   3 paslechoix hdfs         46 2018-02-25 22:08 spark1/salary


[paslechoix@gw03 ~]$ hdfs dfs -cat spark1/mgr1.csv
1, Cliff
2, Raj
3, Alim
4, Jose
5, Jeff
[paslechoix@gw03 ~]$


val mgr1 = sc.textFile("spark1/mgr1.csv") 

scala> mgr1.first
res4: String = 1, Cliff

val mgrMap1 = mgr1.map(x => (x.split(",")(0).toInt, x.split(",")(1).trim))

scala> mgrMap1.first
res11: (Int, String) = (1,Cliff)

scala> mgrMap1.take(5).foreach(println)
(1,Cliff)
(2,Raj)
(3,Alim)
(4,Jose)
(5,Jeff)



[paslechoix@gw03 ~]$ hdfs dfs -cat spark1/sal1.csv
id, Salary
1,100
2,200
3,300
4,400
5,500
[paslechoix@gw03 ~]$



Create salary RDD:

val sal1 = sc.textFile("spark1/sal1.csv")

scala> sal1.first
res2: String = 1, 100

val salMap1 = sal1.map(x=>(x.split(",")(0).toInt, x.split(",")(1).trim.toInt))

scala> salMap1.first
res3: (Int, Int) = (1,100)

scala> salMap1.take(5).foreach(println)
(1,100)
(2,200)
(3,300)
(4,400)
(5,500)


val joined1 = mgrMap1.join(salMap1)
scala> joined
res14: org.apache.spark.rdd.RDD[(Int, (String, Int))] = MapPartitionsRDD[8] at join at <console>:35

val joinedMap1 = joined1.map(x=>(x._1, x._2._1, x._2._2))

step 4 : Join all pairRDDS 
val joined = namePairRDD.join(salaryPairRDD).join(managerPairRDD) 

Step 5 : Now sort the joined results. 
val joinedData = joined.sortByKey() 

Step 6 : Now generate comma separated data. 
val finalData = joinedData.map(v=> (v._1, v._2._1._1,v._2._1._2,v._2._2))

Step 7 : Save this output in hdfs as text file. 
finalData.saveAsTextFile("spark1/result.txt") 

