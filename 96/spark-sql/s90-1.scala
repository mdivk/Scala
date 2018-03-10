Problem Scenario 90 : You have been given below two files 

course.txt 

id,course 
1,Hadoop
2,Spark
3,HBase
5,Impala

Fee.txt 
id,fee 
2,3900
3,4200
4,2900


Accomplish the following activities. 
1. Select all the courses and their fees , whether fee is listed or not. 
2. Select all the available fees and respective course. If course does not exists still list the fee 
3. Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null. 

Solution : 

Step 1 . 
hdfs dfs -mkdir sparksq14 
hdfs dfs -put course.txt sparksql4/ 
hdfs dfs -put tee.txt sparksql4/ 

Step 2 : Now in spark shell 

//load the data into a new ROD 
val course = sc.textFile("sparksql4/course.txt") 
val fee = sc.textFile("sparksql4/fee.txt") 


//define the schema using a case class 
case class Course(id: Integer, name: String) 
scala> case class Fee(id: Int, amount: Float)


//create an RDD ot Product objects 
val courseRDD = course.map(_.split(",")).map(x=>(Course(x(0).toInt, x(1))))
val feeRDD = fee.map(_.split(",")).map(x=>(Fee(x(0).toInt, x(1).toFloat)))
/*

courseRDD.first() 
courseRDD.count() 
feeRDD.first() 
feeRDD.count() 
*/
// change RDD of Product objects to a DataFrame 
val courseDF = courseRDD.toDF() 
val feeDF = feeRDD.toDF() 

// register the DataFrame as a temp table 
courseDF.registerTempTable("course") 
feeDF.registerTempTable("fee") 


/*course.txt 
id,course 
1,Hadoop
2,Spark
3,HBase
5,Impala

Fee.txt 
id,amount 
2,3900
3,4200
4,2900*/

//1. Select all the courses and their fees , whether fee is listed or not. 
sqlContext.sql("select c.id, c.course, isnull(f.amount, 'N/A') from course c left outer join fee f on f.id = c.id").show

sqlContext.sql("select c.id, c.course, f.amount from course c left outer join fee f on f.id = c.id").show
+---+------+------+
| id|course|amount|
+---+------+------+
|  1|Hadoop|  null|
|  2| Spark|3900.0|
|  3| HBase|4200.0|
|  5|Impala|  null|
+---+------+------+


val result = sqlContext.sql("""
  select
    c.id, c.name as course,
    case when f.amount is null then 'N/A' else f.amount end as amount
  from
    course c left outer join fee f on f.id = c.id
""")

+---+------+------+
| id|course|amount|
+---+------+------+
|  1|Hadoop|   N/A|
|  2| Spark|3900.0|
|  3| HBase|4200.0|
|  5|Impala|   N/A|
+---+------+------+



sqlContext.sql("select c.id, c.course, f.amount from course c join fee f on c.id = f.id").show
+---+------+------+
| id|course|amount|
+---+------+------+
|  2| Spark|3900.0|
|  3| HBase|4200.0|
+---+------+------+


sqlContext.sql("select * from course,fee").show
+---+------+---+------+
| id|course| id|amount|
+---+------+---+------+
|  2| Spark|  2|3900.0|
|  3| HBase|  3|4200.0|
+---+------+---+------+

sqlContext.sql("select * from course c right outer join fee f on f.id = c.id").show
+----+------+---+------+
|  id|course| id|amount|
+----+------+---+------+
|   2| Spark|  2|3900.0|
|   3| HBase|  3|4200.0|
|null|  null|  4|2900.0|
+----+------+---+------+


sqlContext.sql("select * from course c left outer join fee f on f.id = c.id").show
+---+------+----+------+
| id|course|  id|amount|
+---+------+----+------+
|  1|Hadoop|null|  null|
|  2| Spark|   2|3900.0|
|  3| HBase|   3|4200.0|
|  5|Impala|null|  null|
+---+------+----+------+


//2. Select all the available fees and respective course. If course does not exists still list the fee 
sqlContext.sql("").show

//3. Select all the courses and their fees , whether fee is listed or not. However, ignore records having tee as null. 
sqlContext.sql("").show
