Problem Scenario 75 : You have been given MySQL DB with following details. 
user=retail_dba 
password=cloudera 
database=retail_db 
table=retail_db.order_items 
jdbc URL = jdbc:mysql://quickstart:3306/retail_db 
Please accomplish following activities. 
1. Copy "retail_db.order_items" table to hdfs in respective directory P90_order_items. 
2. Do the summation of entire revenue in this table using pyspark. 
3. Find the maximum and minimum revenue as well. 
4. Calculate average revenue 
Columns of order_items table : (order_item_id , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price) 

========================================================================= 

//Solution : 
//Step 1 : Import Single table . 

sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=order_items \
--target-dir=p90_order_items

mysql> select count(1) from order_items;
+----------+
| count(1) |
+----------+
|   172198 |
+----------+
1 row in set (0.03 sec)

//Note : Please check you dont have space between before or after ‘=’ sign. 
//Sqoop uses the MapReduce framework to copy data from RDBMS to hdfs 

//Step 2 : Read the data from one of the partition, created using above command. 
hadoop fs -cat p90_order_items/part-m-00000 

//Step 3 : In scala, get the total revenue across all days and orders. 
scala> val entireTableRDD = sc.textFile("p90_order_items")
scala> entireTableRDD.first
res16: String = 1,1,957,1,299.98,299.98


//#Cast string to float 
scala> val extractedRevenueColumnRDD = entireTableRDD.map(rec=>rec.split(",")(4).toFloat) 
scala> extractedRevenueColumnRDD.take(20).foreach(println)
res17: Float = 299.98


//Calculate the max, min, sum and avg

scala> val totalRevenue = extractedRevenueColumnRDD.reduce(_+_)
totalRevenue: Float = 3.4326256E7

val maximumRevenue = extractedRevenueColumnRDD.reduce((max, revenue) => if(max < revenue) revenue else max)
maximumRevenue: Float = 1999.99

val minimumRevenue = extractedRevenueColumnRDD.reduce((min, revenue) => if(min < revenue) min else revenue)
minimumRevenue: Float = 9.99



scala> val Count=extractedRevenueColumnRDD.count()
Count: Long = 172198

scala> val totalRevenue = extractedRevenueColumnRDD.reduce(_+_)
totalRevenue: Float = 3.4326256E7

scala> val averageRev=totalRevenue/Count
averageRev: Float = 199.34178
