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

vim mgr.csv
1, Cliff Barton
2, Raj Mathews
3, Alim SB
4, Jose Ginto
5, Jeff Clark

vim sal.csv
1, 100
2, 200
3, 300
4, 400
5, 500


[paslechoix@gw03 ~]$ hdfs dfs -put mgr.csv spark1/
[paslechoix@gw03 ~]$ hdfs dfs -ls spark1
Found 5 items
-rw-r--r--   3 paslechoix hdfs        100 2018-02-04 17:22 spark1/EmployeeManager.csv
-rw-r--r--   3 paslechoix hdfs         82 2018-02-04 17:21 spark1/EmployeeName.csv
-rw-r--r--   3 paslechoix hdfs         46 2018-02-04 17:22 spark1/Employeesalary
-rw-r--r--   3 paslechoix hdfs         70 2018-02-25 22:00 spark1/mgr.csv
drwxr-xr-x   - paslechoix hdfs          0 2018-02-04 17:51 spark1/result.txt

[paslechoix@gw03 ~]$ hdfs dfs -cat spark1/mgr.csv
1, Cliff Barton
2, Raj Mathews
3, Alim SB
4, Jose Ginto
5, Jeff Clark
[paslechoix@gw03 ~]$


val mgr = sc.textFile("spark1/mgr.csv") 

scala> mgr.first
res4: String = 1, Cliff Barton

val mgrMap = mgr.map(x => (x.split(",")(0).toInt, x.split(",")(1).trim))

scala> mgrMap.first
res11: (Int, String) = (1,Cliff Barton)

scala> mgrMap.take(5).foreach(println)
(1,Cliff Barton)
(2,Raj Mathews)
(3,Alim SB)
(4,Jose Ginto)
(5,Jeff Clark)


[paslechoix@gw03 ~]$ hdfs dfs -cat spark1/sal.csv
id, Salary
1, 100
2, 200
3, 300
4, 400
5, 500
[paslechoix@gw03 ~]$


Create salary RDD:

val sal = sc.textFile("spark1/sal.csv")

val salMap = sal.map(x=>(x.split(",")(0).toInt, x.split(",")(1).trim.toInt))

scala> salMap.first
res3: (Int, Int) = (1,100)

scala> salMap.take(5).foreach(println)
(1,100)
(2,200)
(3,300)
(4,400)
(5,500)


val joined = mgrMap.join(salMap)

step 4 : Join all pairRDDS 
val joined = namePairRDD.join(salaryPairRDD).join(managerPairRDD) 

Step 5 : Now sort the joined results. 
val joinedData = joined.sortByKey() 

Step 6 : Now generate comma separated data. 
val finalData = joinedData.map(v=> (v._1, v._2._1._1,v._2._1._2,v._2._2))

Step 7 : Save this output in hdfs as text file. 
finalData.saveAsTextFile("spark1/result.txt") 

