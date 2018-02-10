Problem Scenario 73 : You have been given data in json format as below. 
{"first_name":"Ankit","last_name".”jain”},  
{"first_name":"Amir",”last_name”.”Khan”} 
{"first_name":"Rajesh", "last_name":"Khanna"} 
{"first_name":"Priynka", "last_name":"Chopra"} 
{"first_name":"Kareena", "last_name":"Kapoor"} 
{"first_name":"Lokesh",”last_name”.”Yadav”} 
Do the following activity 
1. create employee.json file locally. 

RXIE@RXIE-SERVER /
$ scp cygdrive/c/rxie/learning/scala/175Scala/data/employee.json paslechoix@gw01.itversity.com:/home/paslechoix/data


2. Load this file on hdfs 
[paslechoix@gw01 data]$ hdfs dfs -put employee.json spark2

[paslechoix@gw01 data]$ hdfs dfs -cat spark2/employee.json
{"first_name":"Ankit","last_name":"jain"}
{"first_name":"Amir","last_name":"Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh","last_name":"Yadav"}
[paslechoix@gw01 data]$


3. Register this data as a temp table in Spark using Scala. 

val empRDD=sqlContext.jsonFile("spark2/employee.json")
scala> empRDD.take(10).foreach(println)
[Ankit,jain]
[Amir,Khan]
[Rajesh,Khanna]
[Priynka,Chopra]
[Kareena,Kapoor]
[Lokesh,Yadav]


4. Write select query and print this data. 

scala> val empDF = empRDD.toDF("First_Name", "Last_Name")
scala> empDF.registerTempTable("emp")
scala> val result = sqlContext.sql("select First_Name, Last_Name from emp")
[Ankit,jain]
[Amir,Khan]
[Rajesh,Khanna]
[Priynka,Chopra]
[Kareena,Kapoor]
[Lokesh,Yadav]


5. Now save back this selected data in json format. 

scala> result.toJSON.collect.take(10).foreach(println)
{"First_Name":"Ankit","Last_Name":"jain"}
{"First_Name":"Amir","Last_Name":"Khan"}
{"First_Name":"Rajesh","Last_Name":"Khanna"}
{"First_Name":"Priynka","Last_Name":"Chopra"}
{"First_Name":"Kareena","Last_Name":"Kapoor"}
{"First_Name":"Lokesh","Last_Name":"Yadav"}

scala> result.toJSON.saveAsTextFile("spark2/emp.json")


[paslechoix@gw01 ~]$ hdfs dfs -cat spark2/emp.json/*
{"First_Name":"Ankit","Last_Name":"jain"}
{"First_Name":"Amir","Last_Name":"Khan"}
{"First_Name":"Rajesh","Last_Name":"Khanna"}
{"First_Name":"Priynka","Last_Name":"Chopra"}
{"First_Name":"Kareena","Last_Name":"Kapoor"}
{"First_Name":"Lokesh","Last_Name":"Yadav"}