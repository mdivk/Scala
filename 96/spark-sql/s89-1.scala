Problem Scenario 89 : You have been given below patient data in csv format. 

patients.csv 

patientlD,name,dateOtBirth,lastVisitDate 

1001,Ah Teck,1991-12-31,2012-01-20
1002, Kumar, 2011-10-29,2012-09-20 
1003,Ali,2011-01-30,2012-10-21

Accomplish following activities. 

1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
2. Find all the patients who born in 2011
3. Find all the patients age
4. List patients whose last visited more than 60 days ago
5. Select patients 18 years old or younger

Solution : 

// Step 1 . 
hdfs dfs -mkdir sparksq13 
hdfs dfs -put patients.csv sparksq13/ 

// Step 2 : Now in spark shell 

//SQLContext entry point tor working with structured data 
val sqlContext = new org.apache.spark.sql.SQLContext(sc) 
//this is used to implicitly convert an RDD to a DataFrame. 

import sqlContext.implicits. 
//Import Spark SQL data types and Row. 

import org.apache.spark.sql. 
//load the data into a new RDD 


val patients = sc.textFile("sparksql3/patients.csv")

//Return the first element in this RDD 
patients.first() 

//define the schema using a case class 
case class Patient(patientid: Integer, name: String, dateOfBirth:String , lastVisitDate: String) 


scala> case class Patient(patientid: Integer, name: String, dateOfBirth:String , lastVisitDate: String)
defined class Patient


scala> case class Patient(patientid: Int, name: String, dateOfBirth: String, lastvisit:String)
defined class Patient


//create an RDD of Product objects 
val patRDD = patients.map(_.split(",")).map(p => Patient(p(0).toInt,p(1),p(2),p(3))) 

val patRDD0 = patients.map(_.split(",")).map(p=> (p(0).toInt, p(1), p(2), p(3)))

patRDD.first() 
patRDD.count() 


//change RDD of Product objects to a DataFrame 
//if the RDD is created with the class that defines schema, then there is no need to indicate the schema when toDF, otherwise, schema needs to be defined for later sql usage
val patDF = patRDD.toDF() 



//register the DataFrame as a temp table 
patDF.registerTempTable("patients") 

//Select data from table 
val results = sqlContext.sql( "SELECT * from patients" ) 

//display datatrame in a tabular format 
results.show() 


scala> val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

//FROM patients, find all the patients whose lastVisitDate between current time and '2012-09-15' 
val results1 = sqlContext.sql("select * from patients where to_date(lastVisit)  between to_date('2012-09-15') and current_date()")

+---------+-----+-----------+----------+
|patientid| name|dateOfBirth| lastvisit|
+---------+-----+-----------+----------+
|     1002|Kumar| 2011-10-29|2012-09-20|
|     1003|  Ali| 2011-01-30|2012-10-21|
+---------+-----+-----------+----------+


//Find all the patients who born in 2011 
val results2 = sqlContext.sql("select * from patients where to_date(dateOfBirth)  between to_date('2011-01-01') and to_date('2011-12-31') ")
+---------+-----+-----------+----------+
|patientid| name|dateOfBirth| lastvisit|
+---------+-----+-----------+----------+
|     1002|Kumar| 2011-10-29|2012-09-20|
|     1003|  Ali| 2011-01-30|2012-10-21|
+---------+-----+-----------+----------+



//find all the patients age 
val results2 = sqlContext.sql("select dateOfBirth, floor(datediff(current_date(), dateOfBirth)/365) AS Age  from patients")
+---------+-------+-----------+----------+---+
|patientid|   name|dateOfBirth| lastvisit|Age|
+---------+-------+-----------+----------+---+
|     1001|Ah Teck| 1991-12-31|2012-01-20| 26|
|     1002|  Kumar| 2011-10-29|2012-09-20|  6|
|     1003|    Ali| 2011-01-30|2012-10-21|  7|
+---------+-------+-----------+----------+---+



scala> sqlContext.sql("select current_date() as CunnreYear, dateOfBirth, floor(datediff(current_date(), dateOfBirth)/365) AS Age  from patients").show
+----------+-----------+---+
|CunnreYear|dateOfBirth|Age|
+----------+-----------+---+
|2018-03-09| 1991-12-31| 26|
|2018-03-09| 2011-10-29|  6|
|2018-03-09| 2011-01-30|  7|
+----------+-----------+---+


//List patients whose last visited more than 60 days ago 
scala> sqlContext.sql("select * from patients where datediff(current_date(), lastvisit) > 60").show

+---------+-------+-----------+----------+
|patientid|   name|dateOfBirth| lastvisit|
+---------+-------+-----------+----------+
|     1001|Ah Teck| 1991-12-31|2012-01-20|
|     1002|  Kumar| 2011-10-29|2012-09-20|
|     1003|    Ali| 2011-01-30|2012-10-21|
+---------+-------+-----------+----------+


--Select patients 18 years old or younger 
scala> sqlContext.sql("select * from patients where floor(datediff(current_date(), dateOfBirth)/365) < 18 ").show
+---------+-----+-----------+----------+
|patientid| name|dateOfBirth| lastvisit|
+---------+-----+-----------+----------+
|     1002|Kumar| 2011-10-29|2012-09-20|
|     1003|  Ali| 2011-01-30|2012-10-21|
+---------+-----+-----------+----------+