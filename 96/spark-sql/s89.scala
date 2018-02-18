Problem Scenario 89 : You have been given below patient data in csv format. 

Attention:

for date format string, it is yyyy-MM-dd, not yyyy-mm-dd, the system won't recognize mm as the month, which will cause all month be reset to 01


patients.csv 

patientlD,name,dateOtBirth,lastVisitDate 
1001,Ah Teck,1991-12-31,2012-01-20
1002,Kumar,2011-10-29,2012-09-20 
1003,Ali,2011-01-30,2012-10-21

Accomplish following activities. 
1. Find all the patients whose lastVisitDate between current time and '2012-09-15' 
2. Find all the patients who born in 2011 
3. Find all the patients age 
4. List patients whose last visited more than 60 days ago 
5. Select patients 18 years old or younger 


Solution : 

Step 1 :
hdfs dfs -mkdir sparksql3 
hdfs dfs -put patients.csv sparksql3/ 

[paslechoix@gw03 ~]$ hdfs dfs -cat sparksql3/patients.csv
1001,Ah Teck,1991-12-31,2012-01-20
1002,Kumar,2011-10-29,2012-09-20
1003,Ali,2011-01-30,2012-10-21


Step 2 : Now in spark shell 
//SQLContext entry point tor working with structured data 
val sqlContext = new org.apache.spark.sql.SQLContext(sc) 
//this is used to implicitly convert an RDD to a DataFrame. 
import sqlContext.implicits. 
//Import Spark SQL data types and Row. 
import org.apache.spark.sql. 
//load the data into a new RDD 
val patients = sc.textFile("sparksql3/patients.csv") 

scala> patients.first
res1: String = 1001,Ah Teck,1991-12-31,2012-01-20

//define the schema using a case class 
//case class Patient(patientid: Integer, name: String, dateOfBirth:String , lastVisitDate: String) 
//create an RDD of Product objects 
val patRDD = patients.map(p => (p.split(",")(0), p.split(",")(1), p.split(",")(2), p.split(",")(3))) 

scala> patRDD.first
res7: (String, String, String, String) = (1001,Ah Teck,1991-12-31,2012-01-20)

scala> patRDD.count() 
res8: Long = 3

//change RDD of Product objects to a DataFrame 
val patDF = patRDD.toDF("patientID","name","dateOfBirth","lastVisitDate") 
scala> patDF.show
+---------+-------+-----------+-------------+
|patientID|   name|dateOfBirth|lastVisitDate|
+---------+-------+-----------+-------------+
|     1001|Ah Teck| 1991-12-31|   2012-01-20|
|     1002|  Kumar| 2011-10-29|   2012-09-20|
|     1003|    Ali| 2011-01-30|   2012-10-21|
+---------+-------+-----------+-------------+

//register the DataFrame as a temp table 
patDF.registerTempTable("patients") 
//Select data from table 
val results1 = sqlContext.sql("SELECT * from patients") 
//display datatrame in a tabular format 
results1.show() 
+---------+-------+-----------+-------------+
|patientID|   name|dateOfBirth|lastVisitDate|
+---------+-------+-----------+-------------+
|     1001|Ah Teck| 1991-12-31|   2012-01-20|
|     1002|  Kumar| 2011-10-29|   2012-09-20|
|     1003|    Ali| 2011-01-30|   2012-10-21|
+---------+-------+-----------+-------------+


//Find all the patients whose lastVisitDate between current time and '2012-09-15' 
SELECT * FROM patients WHERE from_unixtime(unix_timestamp(lastVisitDate, 'yyyy-mm-dd')) between '2012-09-15' and current_timestamp() order by lastVisitDate
val results2 = sqlContext.sql("SELECT * FROM patients WHERE year(to_date(cast(unix_timestamp(lastVisitDate,'yyyy-mm-dd') as timestamp))) between '2012-09-15' and current_timestamp() order by lastVisitDate")

Error:
18/02/17 15:29:41 INFO ParseDriver: Parsing command: SELECT * FROM patients WHERE year(to_date(cast(unix_timestamp(lastVisitDate,'yyyy-mm-dd') as timestamp))) between '2012-09-15' and current_timestamp() order by lastVisitDate
18/02/17 15:29:41 INFO ParseDriver: Parse Completed
org.apache.spark.sql.AnalysisException: cannot resolve '(year(todate(cast(cast(unixtimestamp(lastVisitDate,yyyy-mm-dd) as timestamp) as date))) <= 1518899382005000)' due to data type mismatch: differing types in '(year(todate(cast(cast(unixtimestamp(lastVisitDate,yyyy-mm-dd) as timestamp) as date))) <= 1518899382005000)' (int and timestamp).; line 0 pos 0

val results3 = sqlContext.sql("SELECT from_unixtime(unix_timestamp(lastVisitDate, 'yyyy-mm-dd')), * FROM patients")
results3.show() 
+-------------------+---------+-------+-----------+-------------+
|                _c0|patientlD|   name|dateOfBirth|lastVisitDate|
+-------------------+---------+-------+-----------+-------------+
|2012-01-20 00:01:00|     1001|Ah Teck| 1991-12-31|   2012-01-20|
|2012-01-20 00:09:00|     1002|  Kumar| 2011-10-29|   2012-09-20|
|2012-01-21 00:10:00|     1003|    Ali| 2011-01-30|   2012-10-21|
+-------------------+---------+-------+-----------+-------------+

val results3 = sqlContext.sql("SELECT from_unixtime(unix_timestamp(lastVisitDate, 'yyyy-MM-dd')), * FROM patients")
results3.show() 
+-------------------+---------+-------+-----------+-------------+
|                _c0|patientID|   name|dateOfBirth|lastVisitDate|
+-------------------+---------+-------+-----------+-------------+
|2012-01-20 00:00:00|     1001|Ah Teck| 1991-12-31|   2012-01-20|
|2012-09-20 00:00:00|     1002|  Kumar| 2011-10-29|   2012-09-20|
|2012-10-21 00:00:00|     1003|    Ali| 2011-01-30|   2012-10-21|
+-------------------+---------+-------+-----------+-------------+


val results2 = sqlContext.sql("SELECT * FROM patients WHERE from_unixtime(unix_timestamp(lastVisitDate, 'yyyy-MM-dd')) between '2012-09-15' and current_timestamp() order by lastVisitDate")
results2.show() 
+---------+-----+-----------+-------------+
|patientID| name|dateOfBirth|lastVisitDate|
+---------+-----+-----------+-------------+
|     1002|Kumar| 2011-10-29|   2012-09-20|
|     1003|  Ali| 2011-01-30|   2012-10-21|
+---------+-----+-----------+-------------+



//Find all the patients who born in 2011 
val results4 = sqlContext.sql("select * from  patients where year(to_date(cast(unix_timesTamp(dateofBirth,'yyyy-MM-dd') as timestamp))) =2011")
results4.show(); 
+---------+-----+-----------+-------------+
|patientID| name|dateOfBirth|lastVisitDate|
+---------+-----+-----------+-------------+
|     1002|Kumar| 2011-10-29|   2012-09-20|
|     1003|  Ali| 2011-01-30|   2012-10-21|
+---------+-----+-----------+-------------+


//find all the patients age 
val results5 = sqlContext.sql("Select name,dateOfBirth,datediff(current_date(), To_date(cast(unix_timestamp(dateOfBirth,'yyyy-MM-dd') as timestamp)))/365 as age FROM patients")
results5.show() 
+-------+-----------+-----------------+
|   name|dateOfBirth|              age|
+-------+-----------+-----------------+
|Ah Teck| 1991-12-31|26.15068493150685|
|  Kumar| 2011-10-29| 6.30958904109589|
|    Ali| 2011-01-30|7.054794520547945|
+-------+-----------+-----------------+


Try to round the age but not successful:

import org.apache.spark.sql.functions
val results5 = sqlContext.sql("Select name,dateOfBirth,datediff(current_date(), functions.round(To_date(cast(unix_timestamp(dateOfBirth,'yyyy-MM-dd')as timestamp)))/365) as age FROM patients")
results5.show() 

Error:
org.apache.spark.sql.AnalysisException: undefined function functions.round; line 1 pos 105


//List patients whose last visited more than 60 days ago 
val results6 = sqlContext.sql("SELECT name, lastVisitdate FROM patients WHERE datediff(current_date(),to_date(cast(unix_timestamp(lastvisitdate, 'yyyy-MM-dd') as timestamp))) > 60")
results6.show() 
+-------+-------------+
|   name|lastVisitdate|
+-------+-------------+
|Ah Teck|   2012-01-20|
|  Kumar|   2012-09-20|
|    Ali|   2012-10-21|
+-------+-------------+

//Select patients 18 years old or younger 

val results7 = sqlContext.sql("select * from patients where to_date(cast(unix_timestamp(dateOfBirth,'yyyy-MM-dd')as timestamp))>date_sub(current_date(),18*365)")
results7.show() 
+---------+-----+-----------+-------------+
|patientID| name|dateOfBirth|lastVisitDate|
+---------+-----+-----------+-------------+
|     1002|Kumar| 2011-10-29|   2012-09-20|
|     1003|  Ali| 2011-01-30|   2012-10-21|
+---------+-----+-----------+-------------+


