Problem Scenario 76 : You have been given MySQL DB with following details. 

User=retail_dba 
password=cloudera 
Database=retail_db 
Table=retail_db.orders 
jdbc URL = jdbc:mysql://quickstart:3306/retail_db 
Columns ot order table : (order_id , order_date , order_customer_id,order_status) 
Please accomplish following activities. 
1. Copy "retail_db.orders" table to hdfs in a directory p91_orders. 
2. Once data is copied to hdfs, using scala calculate the number of order for each status. 
3. use all the following methods to calculate the number of order for each status. (You need to know all these functions and its behavior for real exam) 
- countByKey() 
- groupByKey() 
- reduceByKey() 
- aggregateByKey() 
- combineByKey() 
=========================================================================
Solution : 

mysql> select * from orders limit 10;
+----------+---------------------+-------------------+-----------------+
| order_id | order_date          | order_customer_id | order_status    |
+----------+---------------------+-------------------+-----------------+
|        1 | 2013-07-25 00:00:00 |             11599 | CLOSED          |
|        2 | 2013-07-25 00:00:00 |               256 | PENDING_PAYMENT |
|        3 | 2013-07-25 00:00:00 |             12111 | COMPLETE        |
|        4 | 2013-07-25 00:00:00 |              8827 | CLOSED          |
|        5 | 2013-07-25 00:00:00 |             11318 | COMPLETE        |
|        6 | 2013-07-25 00:00:00 |              7130 | COMPLETE        |
|        7 | 2013-07-25 00:00:00 |              4530 | COMPLETE        |
|        8 | 2013-07-25 00:00:00 |              2911 | PROCESSING      |
|        9 | 2013-07-25 00:00:00 |              5657 | PENDING_PAYMENT |
|       10 | 2013-07-25 00:00:00 |              5648 | PENDING_PAYMENT |
+----------+---------------------+-------------------+-----------------+
10 rows in set (0.00 sec)

//Step 1 : Import Single table 
sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=orders \
--target-dir=p91_orders  

//Step 2: Verify it:
hdfs dfs -cat p91_orders/part-m-00000
68879,2014-07-09 00:00:00.0,778,COMPLETE
68880,2014-07-13 00:00:00.0,1117,COMPLETE
68881,2014-07-19 00:00:00.0,2518,PENDING_PAYMENT
68882,2014-07-22 00:00:00.0,10000,ON_HOLD
68883,2014-07-23 00:00:00.0,5533,COMPLETE

//Step 3 : countByKey 
//#Number of orders by status 
val allOrders = sc.textFile("p91_orders") 
scala> allOrders.count
res0: Long = 68883

scala> allOrders.first
res1: String = 1,2013-07-25 00:00:00.0,11599,CLOSED

//map it: <K, V> = <order_status, (order_id, order_date, order_customer_id, order_status)>
val allOrdersMap = allOrders.map(rec => (rec.split(",")(3), (rec.split(",")(0), rec.split(",")(1).substring(0,10), rec.split(",")(2), rec.split(",")(3)) ))
scala> allOrdersMap.first
res10: (String, (String, String, String, String)) = (CLOSED,(1,2013-07-25,11599,CLOSED))

//Step 4 : groupByKey 
scala> val groupedAllOrdersMap = allOrdersMap.groupByKey
scala> groupedAllOrdersMap.count
res0: Long = 9

val filteredAllOrdersMap = groupedAllOrdersMap.filter(rec => (rec._1 == "CLOSED"))
keyValue = allOrders.map(lambda line: (line.split)”,”)[3],1)) 
#Using countByKey, aggregate data based on status as a key 
output= keyValue.groupByKey().map(lambda kv: (kv[O], sum(kv[l]))) 
for line in output.collect() : print(line) 

Step 5 : reduceByKey 
#Generate key and value pairs (key is order status and vale as an one 
keyValue = allOrders.map(lambda line: (line.split(",”)[3],1)) 
#Using countByKey, aggregate data based on status as a key 
output= keyValue.reduceByKey(lambda a, b: a + b) 
for line in output.collect() : print(line) 

Step 6 : aggregateByKey 
#Generate key and value pairs (key is order status and vale as an one 

keyValue = allOrders.map(lambda line: (line.split(“,”)[3],line)) 
output=keyValue.aggregateByKey(0, lambda a, b: a+l, lambda a, b: a+b) 
for line in output.collect() : print(line) 

Step 7 : combineByKey 
#Generate key and value pairs (key is order status and vale as an one 
keyValue = allOrders.map(lambda line: (line.split(“,”)[3], line)) 
output=keyValue.combineByKey(lambda value: 1, lambda acc, value: acc+l, lambda acc, value: acc+value) 
for line in output.collect() : print(line) 
#Watch Spark Protessional Training provided by www.HadoopExam.com to understand more on each above functions. 
(These are very important functions for real exam)


//DO this with SparkSQL-----------------------------------------------------------------------
scala> val allOrders = sc.textFile("p91_orders") 
scala> allOrders.count
res0: Long = 68883

scala> val allOrdersMap = allOrders.map(rec =>(rec.split(",")(0), rec.split(",")(1).substring(0,10), rec.split(",")(2), rec.split(",")(3)))
//| order_id | order_date          | order_customer_id | order_status
scala> val allOrdersDF = allOrdersMap.toDF("order_id", "order_date", "order_customer_id", "order_status")
scala> allOrdersDF.registerTempTable("orders")
scala> val result = sqlContext.sql("select order_status, count(1) as Total from orders group by order_status")
+---------------+-----+
|   order_status|Total|
+---------------+-----+
|        PENDING| 7610|
|        ON_HOLD| 3798|
| PAYMENT_REVIEW|  729|
|PENDING_PAYMENT|15030|
|     PROCESSING| 8275|
|         CLOSED| 7556|
|       COMPLETE|22899|
|       CANCELED| 1428|
|SUSPECTED_FRAUD| 1558|
+---------------+-----+
