Problem Scenario 91: You have been given data in json format as below. 

{"first_name":"Ankit", "last_name": "Khan"}
{"first_name":"Rajesh", "last name":"Khanna"} 
{"first_name":"Priynka", "last name":"Chopra"} 
{"first_name":"Kareena", "last name":"Kapoor"} 
{"first_name":"Lokesh", "last name":"Yadav"} 

Do the following activity 

1. create employee.json file locally. 
2. Load this tile on hdfs 
3. Register this data as a temp table in Spark using Python. 

4. Write select query and print this data. 
5. Now save back this selected data in json format. 

======================================================================= 

Solution 

Step 1 : create employee.json file locally. 

vi employee.json 

(press insert) 

past the content. 

Step 2 : Upload this file to hdfs, default location 
hadoop fs -put employee.json 
val employee =sqlContext.read.json("/user/cloudera/employee.json”) 
employee.write.parquet("employee.parquet") 

val parq_data = sqlcontext.read.parquet("employee.parquet") 

parq_data.registerTempTable("employee") 

val all_employee = sqlContext.sql("SELeCT * FROM employee") 

all_employee.show() 

import org.apache.spark.sql.SaveMode 

prdDF.write.format("orc").saveAsTable("product_orc_table") 

//Change the codec. 

sqlContext. "snappy") 

employee. write.mode(SaveMode.Overwrite).parquet("employee.parquet") 
