Problem Scenario 78 : You have been given MySQL DB with following details. 

User=retail_dba 
password=cloudera 
Database=retail_db 
table=retail_db.orders 
table=retail_db.order_items 
jdbc URL = jdbc:mysql://quickstart:3306/retail_db 
Columns of order table : (order_id , order_date , order_customer_id, order_status) 
Columns of order_items table : (order_item_id , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
Please accomplish following activities. 
1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory p92_orders and p92_order_items . 
2. Join these data using order_id in Spark and Python 
3. Calculate total revenue perday and per customer 
4. Calculate maximum revenue customer 
======================================================================= 

Solution : 

Step 0: Insepct the data in mysql 

mysql -h ms.itversity.com -u retail_user -p

mysql> desc orders;
+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
| order_status      | varchar(45) | NO   |     | NULL    |                |
+-------------------+-------------+------+-----+---------+----------------+
4 rows in set (0.00 sec)

mysql> desc order_items;
+--------------------------+------------+------+-----+---------+----------------+
| Field                    | Type       | Null | Key | Default | Extra          |
+--------------------------+------------+------+-----+---------+----------------+
| order_item_id            | int(11)    | NO   | PRI | NULL    | auto_increment |
| order_item_order_id      | int(11)    | NO   |     | NULL    |                |
| order_item_product_id    | int(11)    | NO   |     | NULL    |                |
| order_item_quantity      | tinyint(4) | NO   |     | NULL    |                |
| order_item_subtotal      | float      | NO   |     | NULL    |                |
| order_item_product_price | float      | NO   |     | NULL    |                |
+--------------------------+------------+------+-----+---------+----------------+
6 rows in set (0.00 sec)


Step 1 : Import Single table . 


sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=orders \
--target-dir=p92_orders  

18/02/11 14:14:27 INFO mapreduce.ImportJobBase: Retrieved 68883 records.

sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=order_items \
--target-dir=p92_order_items  

18/02/11 14:15:25 INFO mapreduce.ImportJobBase: Retrieved 172198 records.


//Step2: Read the data from one of the partition,created using above command.

[paslechoix@gw01 ~]$ hadoop fs -tail p92_orders/part-m-00000
014-06-12 00:00:00.0,4229,PENDING
68861,2014-06-13 00:00:00.0,3031,PENDING_PAYMENT
68862,2014-06-15 00:00:00.0,7326,PROCESSING
68863,2014-06-16 00:00:00.0,3361,CLOSED
68864,2014-06-18 00:00:00.0,9634,ON_HOLD
68865,2014-06-19 00:00:00.0,4567,SUSPECTED_FRAUD
68866,2014-06-20 00:00:00.0,3890,PENDING_PAYMENT

[paslechoix@gw01 ~]$ hadoop fs -tail p92_order_items/part-m-00000
8868,403,1,129.99,129.99
172168,68869,403,1,129.99,129.99
172169,68869,191,1,99.99,99.99
172170,68869,60,1,999.99,999.99
172171,68870,365,3,179.97,59.99
172172,68870,365,5,299.95,59.99

//Step 3: create RDD for orders and order_items
scala> val ordersRDD = sc.textFile("p92_orders")
scala> ordersRDD.count
res12: Long = 68883

scala> val order_itemsRDD = sc.textFile("p92_order_items")
scala> order_itemsRDD.count
res13: Long = 172198

//Step 4: register the RDDs as DFs
//4.1 convert to map
val ordersRDDMap = ordersRDD.map(rec => (rec.split(",")(0),rec.split(",")(1).substring(0,10),rec.split(",")(2),rec.split(",")(3)))
scala> ordersRDDMap.first
res14: (String, String, String, String) = (1,2013-07-25 00:00:00.0,11599,CLOSED)

val order_itemsRDDMap = order_itemsRDD.map(rec => (rec.split(",")(0),rec.split(",")(1),rec.split(",")(2),rec.split(",")(3),rec.split(",")(4),rec.split(",")(5)))
scala> order_itemsRDDMap.first
res15: (String, String, String, String, String, String) = (1,1,957,1,299.98,299.98)

//4.2 convert to DF and then register as temp tables
val ordersRDDMapDF = ordersRDDMap.toDF("order_id", "order_date", "order_customer_id", "order_status")
scala> ordersRDDMapDF.show
+--------+----------+-----------------+---------------+
|order_id|order_date|order_customer_id|   order_status|
+--------+----------+-----------------+---------------+
|       1|2013-07-25|            11599|         CLOSED|
|       2|2013-07-25|              256|PENDING_PAYMENT|
|       3|2013-07-25|            12111|       COMPLETE|
|       4|2013-07-25|             8827|         CLOSED|
|       5|2013-07-25|            11318|       COMPLETE|
|       6|2013-07-25|             7130|       COMPLETE|
|       7|2013-07-25|             4530|       COMPLETE|
|       8|2013-07-25|             2911|     PROCESSING|
|       9|2013-07-25|             5657|PENDING_PAYMENT|
|      10|2013-07-25|             5648|PENDING_PAYMENT|
|      11|2013-07-25|              918| PAYMENT_REVIEW|
|      12|2013-07-25|             1837|         CLOSED|
|      13|2013-07-25|             9149|PENDING_PAYMENT|
|      14|2013-07-25|             9842|     PROCESSING|
|      15|2013-07-25|             2568|       COMPLETE|
|      16|2013-07-25|             7276|PENDING_PAYMENT|
|      17|2013-07-25|             2667|       COMPLETE|
|      18|2013-07-25|             1205|         CLOSED|
|      19|2013-07-25|             9488|PENDING_PAYMENT|
|      20|2013-07-25|             9198|     PROCESSING|
+--------+----------+-----------------+---------------+
only showing top 20 rows



scala> ordersRDDMapDF.registerTempTable("orders")


val order_itemsRDDMapDF = order_itemsRDDMap.toDF("order_item_id","order_item_order_id", "order_item_product_id", "order_item_quantity","order_item_subtotal","order_item_product_price")
order_itemsRDDMapDF.show(3)
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|            1|                  1|                  957|                  1|             299.98|                  299.98|
|            2|                  2|                 1073|                  1|             199.99|                  199.99|
|            3|                  2|                  502|                  5|              250.0|                    50.0|

scala> order_itemsRDDMapDF.registerTempTable("order_items")



//Now do whatever data analytics needed in Spark SQL
//3. Calculate total revenue perday and per customer 
//4. Calculate maximum revenue customer

val result = sqlContext.sql("select order_customer_id, order_date, round(sum(order_item_subtotal), 2) as revenue_per_customer from orders o join order_items oi on oi.order_item_order_id = o.order_id group by order_customer_id, order_date")
scala> result.take(100).foreach(println)
[1320,2013-08-14 00:00:00.0,439.95]
[9294,2014-03-01 00:00:00.0,819.87]
[8398,2013-12-06 00:00:00.0,179.97]
[10225,2013-09-11 00:00:00.0,329.98]
[10994,2014-06-21 00:00:00.0,129.99]
[5074,2014-04-20 00:00:00.0,299.98]

//sort by revenue_per_customer
val top_result = sqlContext.sql("select order_customer_id, order_date, round(sum(order_item_subtotal), 2) as revenue_per_customer from orders o join order_items oi on oi.order_item_order_id = o.order_id group by order_customer_id, order_date order by revenue_per_customer desc")
top_result.show(1)
+-----------------+----------+--------------------+
|order_customer_id|order_date|revenue_per_customer|
+-----------------+----------+--------------------+
|            10351|2014-03-02|             3579.85|
+-----------------+----------+--------------------+

