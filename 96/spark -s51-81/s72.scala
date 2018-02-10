Problem Scenario 72 : You have been given a table named "employee2" with following detail. 
first_name string 
last_name string 
Write a spark script in scala which read this table and print all the rows and individual column values. 

========================================================================== 
Solution : 

login to mysql to prepare the table:

mysql> create table employee2 ( first_name varchar(20), last_name varchar(20));

mysql> insert into employee2 select emp_name, 'pasle' from employee;

mysql> select * from employee2;
+------------+-----------+
| first_name | last_name |
+------------+-----------+
| kk         | pasle     |
| onk        | pasle     |
| mj         | pasle     |
+------------+-----------+

1. for mysql table: export it to hdfs using sqoop:

sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_export \
--username retail_user \
--password itversity \
--table=employee2 \
--target-dir="spark2/employee2" 

[paslechoix@gw01 ~]$ hdfs dfs -ls spark2/employee2
Found 2 items
-rw-r--r--   3 paslechoix hdfs          0 2018-02-09 23:20 spark2/employee2/_SUCCESS
-rw-r--r--   3 paslechoix hdfs         28 2018-02-09 23:20 spark2/employee2/part-m-00000
[paslechoix@gw01 ~]$ hdfs dfs -cat spark2/employee2/part-m-00000
kk,pasle
onk,pasle
mj,pasle
[paslechoix@gw01 ~]$


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val emp2RDD = sc.textFile("spark2/employee2")

scala> emp2RDD.collect
res0: Array[String] = Array(kk,pasle, onk,pasle, mj,pasle)

scala> val empDF = emp2RDD.toDF("name")
scala> empDF.show
+---------+
|     name|
+---------+
| kk,pasle|
|onk,pasle|
| mj,pasle|
+---------+

scala> emp2DF.registerTempTable("employee2")

val tbl = sqlContext.sql("select name from employee2")
