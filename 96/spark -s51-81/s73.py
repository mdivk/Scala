Problem Scenario 73 : You have been given data in json format as below. 

{"first_name":"Ankit","last_name"."jain"},  
{"first_name":"Amir","last_name"."Khan"} 
{"first_name":"Rajesh", "last_name":"Khanna"} 
{"first_name":"Priynka", "last_name":"Chopra"} 
{"first_name":"Kareena", "last_name":"Kapoor"} 
{"first_name":"Lokesh","last_name"."Yadav"} 

Do the following activity 

1. create employee.json file locally. 
2. Load this file on hdfs 
3. Register this data as a temp table in Spark using Python. 
4. Write select query and print this data. 
5. Now save back this selected data in json format. 

=========================================================================== 

Solution : 
Step 1 : create employee.json tile locally. 

vi employee.json 
(press insert) 

past the content.

[paslechoix@gw03 ~]$ cat employee.json
{"first_name":"Ankit","last_name"."jain"},
{"first_name":"Amir","last_name"."Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh","last_name"."Yadav"}


Step2:Upload this file to hdfs,default location


hadoop fs -put employee.json
[paslechoix@gw03 ~]$ hdfs dfs -cat employee.json
{"first_name":"Ankit","last_name"."jain"},
{"first_name":"Amir","last_name"."Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh","last_name"."Yadav"}


Step3:Write spark script
#Import SQLContext

From pyspark import SQLContext
#Create instance of SQLContext
sqlContext=SQLContext(sc)
#Load json file
Employee=sqlContext.jsonFile(“employee.json")
#Register RDD as a temp table
Employee.registerTempTable(“EmployeeTab")
#Select data from Employee table
emp=sqlContext.sql(“select * from EmployeeTab")
#Iterate data and print
For row in employeeInfo.collect():
Print(row)

Row(_corrupt_record=u'{"first_name":"Ankit","last_name"."jain"},  ', first_name=None, last_name=None)
Row(_corrupt_record=u'{"first_name":"Amir","last_name"."Khan"} ', first_name=None, last_name=None)
Row(_corrupt_record=None, first_name=u'Rajesh', last_name=u'Khanna')
Row(_corrupt_record=None, first_name=u'Priynka', last_name=u'Chopra')
Row(_corrupt_record=None, first_name=u'Kareena', last_name=u'Kapoor')
Row(_corrupt_record=u'{"first_name":"Lokesh","last_name"."Yadav"} ', first_name=None, last_name=None)

Troubleshooting:
The data copied into the json file is corrupted.

{"first_name":"Ankit","last_name":"jain"}
{"first_name":"Amir","last_name":"Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh","last_name":"Yadav"}

Now:

Row(_corrupt_record=None, first_name=u'Ankit', last_name=u'jain')
Row(_corrupt_record=None, first_name=u'Amir', last_name=u'Khan')
Row(_corrupt_record=None, first_name=u'Rajesh', last_name=u'Khanna')
Row(_corrupt_record=None, first_name=u'Priynka', last_name=u'Chopra')
Row(_corrupt_record=None, first_name=u'Kareena', last_name=u'Kapoor')
Row(_corrupt_record=None, first_name=u'Lokesh', last_name=u'Yadav')


Step4:Write data as s Text file

emp.toJSON().saveAsTextFile(“employeeJson1")

Step5:Check whether data has been created or not

Hadoop fs -cat employeeJson1/part* 

[paslechoix@gw03 ~]$ hdfs dfs -cat employeeJson1/*
{"first_name":"Ankit","last_name":"jain"}
{"first_name":"Amir","last_name":"Khan"}
{"first_name":"Rajesh","last_name":"Khanna"}
{"first_name":"Priynka","last_name":"Chopra"}
{"first_name":"Kareena","last_name":"Kapoor"}
{"first_name":"Lokesh","last_name":"Yadav"}

