Problem Scenario 91: You have been given data in json format as below. 
[{"first_name":"Ankit","last_name":"Jain"]
{"first_name":"Amir","last_name":"Khan"}
{"first_name":"Rajesh","last name":"Khanna"}
{"first_name":"Priynka","last name":"Chopra"}
{"first_name":"Kareena","last name":"Kapoor"}
{"first_name":"Lokesh","last name":"Yadav"}

Do the following activity 
1. create employee.json file locally. 
2. Load this tile on hdfs 
3. Register this data as a temp table in Spark using Scala. 

4. Write select query and print this data. 
5. Now save back this selected data in json format. 

======================================================================= 

Solution 

Step 1 : create employee.json file locally. 
vi employee.json 

Step 2 : Upload this file to hdfs, default location 
[paslechoix@gw03 ~]$ hdfs dfs -cat employee.json
{"first_name":"Ankit","last_name":"jain"}
{"first_name":"Amir","last_name":"Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh","last_name":"Yadav"}


val employee =sqlContext.read.json("employee.json") 

scala> employee.collect
res25: Array[org.apache.spark.sql.Row] = Array([Ankit,jain], [Amir,Khan], [Rajesh,Khanna], [Priynka,Chopra], [Kareena,Kapoor], [Lokesh,Yadav])

scala> employee
res28: org.apache.spark.sql.DataFrame = [first_name: string, last_name: string]

scala> employee.show
+----------+---------+
|first_name|last_name|
+----------+---------+
|     Ankit|     jain|
|      Amir|     Khan|
|    Rajesh|   Khanna|
|   Priynka|   Chopra|
|   Kareena|   Kapoor|
|    Lokesh|    Yadav|
+----------+---------+


employee.write.parquet("employee.parquet") 
[paslechoix@gw03 ~]$ hdfs dfs -cat employee.parquet/*
PAR1<H
      spark_schema
                  %
first_name%
           %    last_name%
                          )org.apache.spark.sql.parquet.row.metadata▒{"type":"struct","fields":[{"name":"first_name","type":"string","nullable":true,"metadata":{}},{"name":"last_name","type":"string","nullable":true,"metadata":{}}]}parquet-mr version 1.6.0-PAR1PAR1<H
                                                       spark_schema
                                                                   %
first_name%
           %    last_name%
                          ,,<part-r-00000-e9bcfe89-5e23-4a45-9e7e-4ca79e8be6e6.gz.parquet



val parq_data = sqlContext.read.parquet("employee.parquet") 
scala> parq_data.show

+----------+---------+
|first_name|last_name|
+----------+---------+
|     Ankit|     jain|
|      Amir|     Khan|
|    Rajesh|   Khanna|
|   Priynka|   Chopra|
|   Kareena|   Kapoor|
|    Lokesh|    Yadav|
+----------+---------+




parq_data.registerTempTable("employee") 

val all_employee = sqlContext.sql("SELECT * FROM employee") 

all_employee.show() 
+----------+---------+
|first_name|last_name|
+----------+---------+
|     Ankit|     jain|
|      Amir|     Khan|
|    Rajesh|   Khanna|
|   Priynka|   Chopra|
|   Kareena|   Kapoor|
|    Lokesh|    Yadav|
+----------+---------+


import org.apache.spark.sql.SaveMode 

prdDF.write.format("orc").saveAsTable("product_orc_table") 

//Change the codec. 

sqlContext."snappy") 

employee. write.mode(SaveMode.Overwrite).parquet("employee.parquet") 

employee. write.mode(SaveMode.Overwrite).parquet("employee.parquet") 