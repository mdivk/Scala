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
sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_export \
--username retail_user \
--password itversity \
--table=orders \
--target-dir=p89_orders 


[paslechoix@gw01 ~]$ hdfs dfs -ls p89_orders
Found 2 items
-rw-r--r--   3 paslechoix hdfs          0 2018-02-10 08:00 p89_orders/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        168 2018-02-10 08:00 p89_orders/part-m-00000

[paslechoix@gw01 ~]$ hdfs dfs -cat p89_orders/part-m-00000
100000,2017-10-31 00:00:00.0,100000,DUMMY
100000,2017-10-10 00:00:00.0,10000,Dummy
100000,2017-10-10 00:00:00.0,10000,Dummy
1111111,2013-07-25 00:00:00.0,257154,CLOSED




sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_export \
--username retail_user \
--password itversity \
--table=order_items \
--target-dir=p89_order_items 

//There is no existing table in the database, copy data from an existing source to data/order_items


[paslechoix@gw01 ~]$ hdfs dfs -ls p89_order_items
Found 1 items
-rw-r--r--   3 paslechoix hdfs    5408880 2018-02-10 08:59 p89_order_items/part-00000



2. Join these data using order_id in Spark and Scala
val ordersRDD = sc.textFile("p89_orders") 

scala> ordersRDD.take(10).foreach(println)
100000,2017-10-31 00:00:00.0,100000,DUMMY
100000,2017-10-10 00:00:00.0,10000,Dummy
100000,2017-10-10 00:00:00.0,10000,Dummy
1111111,2013-07-25 00:00:00.0,257154,CLOSED


scala> ordersRDD.count
res12: Long = 4

val orderltemsRDD = sc.textFile("p89_order_items") 

scala> orderltemsRDD.count
res15: Long = 172198

scala> val ordersRDDMap = ordersRDD.map(rec => (rec.split(" ")(0), rec))
scala> val orderltemsRDDMap = orderltemsRDD.map(rec => (rec.split(" ")(0), rec))
val result = ordersRDDMap.join(orderltemsRDDMap)

//Too less records in orders, no result after joined the two RDD 
scala> result.count
res18: Long = 0

//---------------------------------------------------------------------------------
//Repeat using another orders table
[paslechoix@gw01 orders]$ ls
orders.csv
[paslechoix@gw01 orders]$ wc -l orders.csv
68883 orders.csv

[paslechoix@gw01 orders]$ hdfs dfs -ls p89_orders_new
Found 1 items
-rw-r--r--   3 paslechoix hdfs    2999944 2018-02-10 20:11 p89_orders_new/orders.csv

scala> val orders_newRDD = sc.textFile("p89_orders_new/orders.csv") 
scala> orders_newRDD.count
res0: Long = 68883

scala> val orders_newRDDMap = orders_newRDD.map(rec => (rec.split(",")(0), rec))
scala> val orderltemsRDD = sc.textFile("p89_order_items") 
//For order_items data, the second fields is order_id, to join it with orders, we need to make it as the key 
scala> val orderltemsRDDMap = orderltemsRDD.map(rec => (rec.split(",")(1), rec)) 
scala> val result = orders_newRDDMap.join(orderltemsRDDMap)
scala> result.count
res11: Long = 172198

//Note: 1. the csv is delimitered by comma, not space 
//the common key is order_id, in order_items csv, it is the second field 
scala> result.first
res12: (String, (String, String)) = (2828,(2828,2013-08-10 00:00:00.0,4952,SUSPECTED_FRAUD,7097,2828,403,1,129.99,129.99))

scala> val first = result.first

//OrderID
scala> first._1
res13: String = 2828

scala> first._2._1
res14: String = 2828,2013-08-10 00:00:00.0,4952,SUSPECTED_FRAUD

scala> first._2._2
res17: String = 7097,2828,403,1,129.99,129.99

//order date
scala> first._2._1.split(",")(1).substring(0,10)
res21: String = 2013-08-10

//order amount 
scala> first._2._2.split(",")(4)
res22: String = 129.99


3. Now fetch selected columns from joined data Orderld, Order date and amount collected on this order. 

scala> val final_result = result.map(rec=>(rec._1 + "," + rec._2._1.split(",")(1).substring(0,10) + "," + rec._2._2.split(",")(4)))
scala> final_result.first
res23: String = 2828,2013-08-10,129.99

4. Calculate total order placed for each date, and produced the output sorted by date. 

scala> final_result.take(20).foreach(println)
2828,2013-08-10,129.99
43399,2014-04-20,100.0
43399,2014-04-20,129.99
43399,2014-04-20,49.98
8989,2013-09-19,119.97
8989,2013-09-19,299.97
8989,2013-09-19,299.97
8989,2013-09-19,111.96
20554,2013-11-29,399.98
20554,2013-11-29,119.97
20554,2013-11-29,399.98
20554,2013-11-29,399.96
36070,2014-03-03,100.0
36070,2014-03-03,199.99
36070,2014-03-03,149.94
36070,2014-03-03,299.95
36070,2014-03-03,119.98
17422,2013-11-10,129.99
17422,2013-11-10,239.96
17422,2013-11-10,129.99

scala> val final_resultMap1 = final_result.map(x=>(x.split(",")(1), x.split(",")(2).toFloat))
scala> final_resultMap1.take(20).foreach(println)
(2013-08-10,129.99)
(2014-04-20,100.0)
(2014-04-20,129.99)
(2014-04-20,49.98)
(2013-09-19,119.97)
(2013-09-19,299.97)
(2013-09-19,299.97)
(2013-09-19,111.96)
(2013-11-29,399.98)
(2013-11-29,119.97)
(2013-11-29,399.98)
(2013-11-29,399.96)
(2014-03-03,100.0)
(2014-03-03,199.99)
(2014-03-03,149.94)
(2014-03-03,299.95)
(2014-03-03,119.98)
(2013-11-10,129.99)
(2013-11-10,239.96)
(2013-11-10,129.99)



scala> val sorted_final_resultMap1 = final_resultMap1.sortByKey()
scala> val grouped_final_resultMap1 = final_resultMap1.groupByKey()

scala> grouped_final_resultMap1.first
res34: (String, Iterable[Float]) = (2014-02-01,CompactBuffer(239.96, 129.99, ...

val sumed = final_resultMap1.reduceByKey((_: Float) + (_: Float))         

5. Sort by order date, and then by order amount 
scala> result.first
res12: (String, (String, String)) = (2828,(2828,2013-08-10 00:00:00.0,4952,SUSPECTED_FRAUD,7097,2828,403,1,129.99,129.99))

//create a new RDD with tuple like (order_date, (order_id, order_revenue))
val new_result = result.map(x => (x._2._1.split(",")(1).substring(0,10), (x._1,x._2._2.split(",")(4))))

(2013-08-10 00:00:00.0,(2828,129.99))

//now sort the new result based on the tuple element: order_date, and then order_revenue in desc
val sorted_new_result = new_result.sortBy(x => (x._1,true,x._2._2,false))
(2013-07-25,(57762,99.96))
(2013-07-25,(96,99.96))
(2013-07-25,(14,99.96))
(2013-07-25,(63,99.96))
(2013-07-25,(10,99.96))
(2013-07-25,(57776,99.99))
(2013-07-25,(57776,99.99))
(2013-07-25,(77,99.99))
(2013-07-25,(28,99.99))
(2013-07-25,(101,99.99))
(2013-07-25,(43,99.99))
(2013-07-26,(141,100.0))
(2013-07-26,(213,100.0))
(2013-07-26,(334,100.0))
(2013-07-26,(161,100.0))
(2013-07-26,(220,100.0))
(2013-07-26,(181,100.0))
(2013-07-26,(230,100.0))
(2013-07-26,(57799,100.0))
(2013-07-26,(263,100.0))

val grouped_final_resultMap = grouped_final_result1.map{case (str, nums) => (str, nums.sum)}

6. Group by order date
val grouped_sorted_new_result = sorted_new_result.groupByKey

scala> grouped_sorted_new_result.first
res7: (String, Iterable[(String, String)]) = (2014-02-01,CompactBuffer((30784,100.0), (30865,100.0), (31011,100.0), (30823,100.0), (30780,100.0), (30934,100.0), (30938,100.0), (68085,100.0), (62807,100.0), (30879,100.0), (62805,100.0), (30792,109.95), (30982,111.96), (30782,119.96), (30953,119.97), (30874,119.97), (30870,119.97), (30870,119.97), (30780,119.97), (30876,119.97), (31013,119.97), (30983,119.97), (30916,119.97), (30781,119.97), (68083,119.97), (30808,119.97), (30981,119.97), (62796,119.98), (30850,119.98), (62781,119.98), (30964,119.98), (31008,119.98), (30889,119.98), (30955,119.98), (30894,119.98), (30799,119.98), (30978,119.98), (30822,119.98), (30916,119.98), (30781,119.98), (30820,119.98), (31003,119.98), (30956,119.98), (62799,119.98), (30864,119.98), (30817,119.98), (...

scala> first._2.size
res10: Int = 757

//for 2014-02-01, there are 757 orders 
7. Group by order date, and then sort by submount in desc order

//do this in RDD via redeuce:
//---------------------------------------------------------------------



//do this in Spark-SQL
//---------------------------------------------------------------------
scala> val final_resultMap = final_result.map(x=> (x.split(",")(0),x.split(",")(1),x.split(",")(2)))

scala> final_resultMap.first
res27: (String, String, String) = (2828,2013-08-10,129.99)


scala> val final_resultMapDF = final_resultMap.toDF("order_id","order_date","order_revenue")
scala> final_resultMapDF.show
+--------+----------+-------------+
|order_id|order_date|order_revenue|
+--------+----------+-------------+
|    2828|2013-08-10|       129.99|
|   43399|2014-04-20|        100.0|
|   43399|2014-04-20|       129.99|
|   43399|2014-04-20|        49.98|
|    8989|2013-09-19|       119.97|
|    8989|2013-09-19|       299.97|
|    8989|2013-09-19|       299.97|
|    8989|2013-09-19|       111.96|
|   20554|2013-11-29|       399.98|
|   20554|2013-11-29|       119.97|
|   20554|2013-11-29|       399.98|
|   20554|2013-11-29|       399.96|
|   36070|2014-03-03|        100.0|
|   36070|2014-03-03|       199.99|
|   36070|2014-03-03|       149.94|
|   36070|2014-03-03|       299.95|
|   36070|2014-03-03|       119.98|
|   17422|2013-11-10|       129.99|
|   17422|2013-11-10|       239.96|
|   17422|2013-11-10|       129.99|
+--------+----------+-------------+
only showing top 20 rows


final_resultMapDF.registerTempTable("result1")
val final_result1 = sqlContext.sql("select order_date, round(sum(order_revenue),2) as daily_revenue from result1 group by order_date")
scala> final_result1.show
+----------+-------------+
|order_date|daily_revenue|
+----------+-------------+
|2013-08-06|    120573.66|
|2014-01-20|     96868.09|
|2013-12-10|     80646.53|
|2013-12-11|    128392.47|
|2013-08-07|    103351.39|
|2014-01-21|    124100.85|
|2013-12-12|     77171.26|
|2014-01-22|    104889.78|
|2013-08-08|     76501.66|
|2014-01-23|     108225.0|
|2013-12-13|     60089.17|
|2013-08-09|     62316.47|
|2013-12-14|     52397.76|
|2014-04-30|     65257.96|
|2014-01-24|     86299.03|
|2013-12-15|    108593.97|
|2014-01-25|     56422.01|
|2013-12-16|     47967.21|
|2014-01-26|     75434.02|
|2013-12-17|     62424.04|
+----------+-------------+
only showing top 20 rows


//If need to drop any column, using the following example
val final_resultMapDF1 = final_resultMap.toDF("order_id","order_date","order_revenue").drop("order_id")
scala> final_resultMapDF1.show
+----------+-------------+
|order_date|order_revenue|
+----------+-------------+
|2013-08-10|       129.99|
|2014-04-20|        100.0|
|2014-04-20|       129.99|
|2014-04-20|        49.98|
|2013-09-19|       119.97|
|2013-09-19|       299.97|
|2013-09-19|       299.97|
|2013-09-19|       111.96|
|2013-11-29|       399.98|
|2013-11-29|       119.97|
|2013-11-29|       399.98|
|2013-11-29|       399.96|
|2014-03-03|        100.0|
|2014-03-03|       199.99|
|2014-03-03|       149.94|
|2014-03-03|       299.95|
|2014-03-03|       119.98|
|2013-11-10|       129.99|
|2013-11-10|       239.96|
|2013-11-10|       129.99|
+----------+-------------+
only showing top 20 rows