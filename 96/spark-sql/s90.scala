Problem Scenario 90 : You have been given below two files 

course.txt 
id,course 
1,Hadoop 
2,Spark 
3,HBase 

Fee.txt 
id,fee 
2,3900 
3,4200 
4,2900 

Accomplish the following activities. 
1. Select all the courses and their fees , whether fee is listed or not. 
2. Select all the available fees and respective course. If course does not exists still list the fee 
3. Select all the courses and their fees , whether tee is listed or not. However, ignore records having tee as null. 

Solution : 

Step 1 . 
hdfs dfs -mkdir sparksql4 
hdfs dfs -put course.txt sparksql4/ 
hdfs dfs -put tee.txt sparksql4/ 

Step 2 : Now in spark shell 
//load the data into a new ROD 
val course = sc.textFile("sparksql4/course.txt") 
val fee = sc.textFile("sparksql4/fee.txt") 
//Return the first element in this RDD 
course.first() 
fee.first() 
//define the schema using a case class 
case class Course(id: Integer, name: String) 
case class Fee(id: Integer, fee: Integer) 
//create an RDD ot Product objects 
val courseRDD = course.map(x=>(x.split(",")(0).toInt, x.split(",")(1)))
val feeRDD = fee.map(x=> (x.split(",")(0).toInt, x.split(",")(1).toInt))

// change RDD of Product objects to a DataFrame 
val courseDF = courseRDD.toDF("id", "course") 
val feeDF = feeRDD.toDF("id", "fee") 

// register the DataFrame as a temp table 
courseDF.registerTempTable("course") 
feeDF.registerTempTable("fee") 

// Select data from table 
val results1 = sqlContext.sql( "SELECT * FROM course") 
results1.show() 
+---+------+
| id|course|
+---+------+
|  1|Hadoop|
|  2| Spark|
|  3| HBase|
+---+------+


val results2 = sqlContext.sql( "SELECT * FROM fee") 
results2.show() 
+---+----+
| id| fee|
+---+----+
|  2|3900|
|  3|4200|
|  4|2900|
+---+----+


val results3 = sqlContext.sql( "SELECT * FROM course LEFT JOIN fee ON course.id = fee.id") 
results3.show() 
+---+------+----+----+
| id|course|  id| fee|
+---+------+----+----+
|  1|Hadoop|null|null|
|  2| Spark|   2|3900|
|  3| HBase|   3|4200|
+---+------+----+----+

val results4 = sqlContext.sql("SELECT * FROM course RIGHT JOIN fee ON course.id = fee.id") 
results4.show() 
+----+------+---+----+
|  id|course| id| fee|
+----+------+---+----+
|   2| Spark|  2|3900|
|   3| HBase|  3|4200|
|null|  null|  4|2900|
+----+------+---+----+


val results5 = sqlContext.sql("SELECT * FROM course LEFT JOIN fee ON course.id = fee.id where fee.id IS NULL") 
results5.show() 
+---+------+----+----+
| id|course|  id| fee|
+---+------+----+----+
|  1|Hadoop|null|null|
+---+------+----+----+

