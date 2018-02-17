Problem Scenario 74 : You have been given MySQL DB with following details. 
User=retail_dba 
password=cloudera 
database=retail_db 
table=retail_db.orders 
Table=retail_db.order_items 
jdbc URL = jdbc:mysql://quickstart:3306/retail_db 
Columns of order table : (order_id , order_date , order_customer_id, order_status) 

Columns of order_items table : (order_item_id , order_item_order_ 
id , order_item_product_id, order_item_quantity,order_item_subtotal,order_
item_product_price) 
Please accomplish following activities. 
1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory p89_orders and p89_order_items . 
2. Join these data using order_id in Spark and Python 
3. Now fetch selected columns from joined data Orderld, Order date and amount collected on this order. 
4. Calculate total order placed for each date, and produced the output sorted by date. 
=====================================================================

Solution : 

Step 1 : Import Single table . 

[paslechoix@gw03 ~]$ sqoop import -m=1 --connect=jdbc:mysql://ms.itversity.com/retail_db --username=retail_user --password=itversity --table=orders --target-dir=p89_orders

[paslechoix@gw03 ~]$ hdfs dfs -cat p89_orders/*
68876,2014-07-06 00:00:00.0,4124,COMPLETE
68877,2014-07-07 00:00:00.0,9692,ON_HOLD
68878,2014-07-08 00:00:00.0,6753,COMPLETE
68879,2014-07-09 00:00:00.0,778,COMPLETE
68880,2014-07-13 00:00:00.0,1117,COMPLETE
68881,2014-07-19 00:00:00.0,2518,PENDING_PAYMENT
68882,2014-07-22 00:00:00.0,10000,ON_HOLD
68883,2014-07-23 00:00:00.0,5533,COMPLETE


[paslechoix@gw03 ~]$ sqoop import -m=1 --connect=jdbc:mysql://ms.itversity.com/retail_db --username=retail_user --password=itversity --table=order_items --target-dir=p89_order_items
[paslechoix@gw03 ~]$ hdfs dfs -cat p89_order_items/*
152860,61113,957,1,299.98,299.98
152861,61114,1014,5,249.9,49.98
152862,61115,725,1,108.0,108.0
152863,61115,1073,1,199.99,199.99
152864,61115,191,4,399.96,99.99
152865,61115,1014,4,199.92,49.98


Step 2 : Load these above two directory as RDD using Spark and Python (Open pyspark terminal and do following). 

>>> orders = sc.textFile("p89_orders") 
>>> orders.count()
68883

[paslechoix@gw03 ~]$ hdfs dfs -tail p89_orders/part-m-00000
014-06-12 00:00:00.0,4229,PENDING
68861,2014-06-13 00:00:00.0,3031,PENDING_PAYMENT
68862,2014-06-15 00:00:00.0,7326,PROCESSING
68863,2014-06-16 00:00:00.0,3361,CLOSED
68864,2014-06-18 00:00:00.0,9634,ON_HOLD
68865,2014-06-19 00:00:00.0,4567,SUSPECTED_FRAUD
68866,2014-06-20 00:00:00.0,3890,PENDING_PAYMENT
68867,2014-06-23 00:00:00.0,869,CANCELED
68868,2014-06-24 00:00:00.0,10184,PENDING
68869,2014-06-25 00:00:00.0,7456,PROCESSING


>>> orderltems = sc.textFile("p89_order_items") 
>>> orderltems.count()
172198
[paslechoix@gw03 ~]$ hdfs dfs -tail p89_order_items/part-00000
8868,403,1,129.99,129.99
172168,68869,403,1,129.99,129.99
172169,68869,191,1,99.99,99.99
172170,68869,60,1,999.99,999.99
172171,68870,365,3,179.97,59.99
172172,68870,365,5,299.95,59.99
172173,68871,957,1,299.98,299.98
172174,68871,502,4,200.0,50.0
172175,68873,365,2,119.98,59.99
172176,68873,365,1,59.99,59.99



Step 4 : Convert RDD info key value as (order_id as a key and rest of the values as a value) 
#First value is order_id 
>>> ordersMap = orders.map(lambda line: int(line.split(",")[0]), line)
>>> for line in ordersMap.collect():print(line)
(u'35460', u'35460,2014-02-28 00:00:00.0,10358,COMPLETE')
(u'35461', u'35461,2014-02-28 00:00:00.0,7017,CLOSED')
(u'35462', u'35462,2014-02-28 00:00:00.0,1805,CANCELED')
(u'35463', u'35463,2014-02-28 00:00:00.0,3653,ON_HOLD')
(u'35464', u'35464,2014-02-28 00:00:00.0,1884,ON_HOLD')
(u'35465', u'35465,2014-02-28 00:00:00.0,5445,PROCESSING')
(u'35466', u'35466,2014-02-28 00:00:00.0,10141,PENDING_PAYMENT')


#Second value as an Order id 
>>> orderltemsMap = orderltems.map(lambda line: (int(line.split(",")[1]),line))
>>> for line in orderltemsMap.collect():print(line)
(13559, u'33978,13559,627,3,119.97,39.99')
(13559, u'33979,13559,365,1,59.99,59.99')
(13559, u'33980,13559,1004,1,399.98,399.98')
(13559, u'33981,13559,365,3,179.97,59.99')
(13561, u'33982,13561,957,1,299.98,299.98')
(13561, u'33983,13561,403,1,129.99,129.99')
(13561, u'33984,13561,1004,1,399.98,399.98')


Step 5 : Join both the RDD using order_id 
joinedData = orderltemsMap.join(ordersMap) 
>>> for line in joinedData.collect():print(line)

joinedData1 = orderMap.join(orderItemsMap) 
>>> for line in joinedData.collect():print(line)

#print the joined data 
for line in joinedData.collect(): 
print(line) 
Format of joinedData as below. 
[Orderid, 'All columns from orderltemsKeyValue' , 'All columns from ordersKeyValue'] 

Step 6 : Now fetch selected values Orderld, Order date and amount collected on this order. 
revenuePerOrderPerDay = joinedData.map(lambda row: (row[O],row[1][1].split(“,”)[1],float(row[1][0].split(“,”)[4]))) 
#print the result 
for line in revenuePerOrderPerDay.collect(): 
print(line) 

Step 7 : Select distinct order ids for each date. 
#distinct(date,order_id) 
distinctOrdersDate = joinedData.map(lambda row:row[1][1].split(“,”)[1]+”,”+str(row[0])).distinct() 
for line in distinctOrdersDate.collect(): 

print(line) 

Step 8 : Similar to word count , generate (date, 1) record for each row. 
newLineTuple = distinctOrdersDate.map(lambda line: (line.split(",”)[0],1)) 

Step 9 : Do the count for each key(date), to get total order per date. 
totalOrdersPerDate = newLineTuple.reduceByKey(lambda a, b: a + b) 
#print results 
for line in totalOrdersPerDate.collect(): 
print(line) 

Step 10 : Sort the results by date 
sortedData=totalOrdersPerDate.sortByKey().collect() 
#print results 
for line in sortedData: 
print(line) 
