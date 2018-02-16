Problem Scenario 72 : You have been given a table named "employee2" with following detail. 
first_name string 
last_name string 
Write a spark script in python which read this table and print all the rows and individual column values. 

========================================================================== 
Solution : 
Step 1 : Import statements for HiveContext 

from pyspark.sql import HiveContext 

Step 2 : Create sqlContext 

sqlContext = HiveContext(sc) 

Step 3 : Query hive 

>>> customers = sqlContext.sql("select * from paslechoix.customers")


Step 4 : Now prints the data 

for row in customers.collect(): 
print(row) 
Row(customer_id=12434, customer_fname=u'Mary', customer_lname=u'Mills', customer_email=u'XXXXXXXXX', customer_password=u'XXXXXXXXX', customer_street=u'9720 Colonial Parade', customer_city=u'Caguas', customer_state=u'PR', customer_zipcode=u'00725')
Row(customer_id=12435, customer_fname=u'Laura', customer_lname=u'Horton', customer_email=u'XXXXXXXXX', customer_password=u'XXXXXXXXX', customer_street=u'5736 Honey Downs', customer_city=u'Summerville', customer_state=u'SC', customer_zipcode=u'29483')
.....

Step 5 : Print specific column 

>>> for row in customers.collect():print(row.customer_fname)
Hannah
Mary
Angela
Benjamin
Mary
Laura
