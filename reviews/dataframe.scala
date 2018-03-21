/* Data Frame
DataFrame has two main advantages over RDD:
Optimized execution plans via Catalyst Optimizer.
Custom Memory management via Project Tungsten.

Read more at https://indatalabs.com/blog/data-engineering/convert-spark-rdd-to-dataframe-dataset#2SaMI07DPTd6X6jm.99

 */
//writing data into difference file format

//read data into a dataframe
val ordersDF = sqlContext.read.json("/public/retail_db_json/orders")
ordersDF.first
res0: org.apache.spark.sql.Row = [11599,2013-07-25 00:00:00.0,1,CLOSED]

//supported format 
/* scala> ordersDF.write.
asInstanceOf   format         insertInto     isInstanceOf   jdbc           json           mode
option         options        orc            parquet        partitionBy    save           saveAsTable
text           toString
 */

//------------------parquet----------------------
//Write data into parquet format 
ordersDF.save("/user/paslechoix/orders_parquet", "parquet")
[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/orders_parquet
Found 5 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-26 22:34 /user/paslechoix/orders_parquet/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        495 2018-01-26 22:34 /user/paslechoix/orders_parquet/_common_metadata
-rw-r--r--   3 paslechoix hdfs       1636 2018-01-26 22:34 /user/paslechoix/orders_parquet/_metadata
-rw-r--r--   3 paslechoix hdfs     157619 2018-01-26 22:34 /user/paslechoix/orders_parquet/part-r-00000-74e456a1-4ca1-4425-bb67-316f36e53e30.gz.parquet
-rw-r--r--   3 paslechoix hdfs     159746 2018-01-26 22:34 /user/paslechoix/orders_parquet/part-r-00001-74e456a1-4ca1-4425-bb67-316f36e53e30.gz.parquet

//data verification
scala> sqlContext.load

def load(path: String): DataFrame
def load(path: String, source: String): DataFrame
def load(source: String, options: immutable.Map[String,String]): DataFrame
def load(source: String, options: java.util.Map[String,String]): DataFrame
def load(source: String, schema: types.StructType, options: immutable.Map[String,String]): DataFrame
def load(source: String, schema: types.StructType, options: java.util.Map[String,String]): DataFrame

sqlContext.load("/user/paslechoix/orders_parquet", "parquet").show(10)
+-----------------+--------------------+--------+---------------+
|order_customer_id|          order_date|order_id|   order_status|
+-----------------+--------------------+--------+---------------+
|            11599|2013-07-25 00:00:...|       1|         CLOSED|
|              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
|            12111|2013-07-25 00:00:...|       3|       COMPLETE|
|             8827|2013-07-25 00:00:...|       4|         CLOSED|
|            11318|2013-07-25 00:00:...|       5|       COMPLETE|
|             7130|2013-07-25 00:00:...|       6|       COMPLETE|
|             4530|2013-07-25 00:00:...|       7|       COMPLETE|
|             2911|2013-07-25 00:00:...|       8|     PROCESSING|
|             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
|             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
+-----------------+--------------------+--------+---------------+

//Write data into parquet format, method 2, using write.parquet
scala> ordersDF.write.parquet("/user/paslechoix/orders_parquet2")
sqlContext.load("/user/paslechoix/orders_parquet2", "parquet").show(10)
+-----------------+--------------------+--------+---------------+
|order_customer_id|          order_date|order_id|   order_status|
+-----------------+--------------------+--------+---------------+
|            11599|2013-07-25 00:00:...|       1|         CLOSED|
|              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
|            12111|2013-07-25 00:00:...|       3|       COMPLETE|
|             8827|2013-07-25 00:00:...|       4|         CLOSED|
|            11318|2013-07-25 00:00:...|       5|       COMPLETE|
|             7130|2013-07-25 00:00:...|       6|       COMPLETE|
|             4530|2013-07-25 00:00:...|       7|       COMPLETE|
|             2911|2013-07-25 00:00:...|       8|     PROCESSING|
|             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
|             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
+-----------------+--------------------+--------+---------------+

//----------------orc-------------------
//Write data into orc format 
ordersDF.save("/user/paslechoix/orders_orc", "orc")

//verify on hdfs first
[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/orders_orc
[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/orders_orc
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-26 22:53 /user/paslechoix/orders_orc/_SUCCESS
-rw-r--r--   3 paslechoix hdfs      81334 2018-01-26 22:53 /user/paslechoix/orders_orc/part-r-00000-fd2bb5f7-f24a-49e7-9813-56f15cdefbbd.orc
-rw-r--r--   3 paslechoix hdfs      83256 2018-01-26 22:53 /user/paslechoix/orders_orc/part-r-00001-fd2bb5f7-f24a-49e7-9813-56f15cdefbbd.orc
//Verify by loading it to data frame and show it
sqlContext.load("/user/paslechoix/orders_orc", "orc").show(10)
+-----------------+--------------------+--------+---------------+
|order_customer_id|          order_date|order_id|   order_status|
+-----------------+--------------------+--------+---------------+
|            11599|2013-07-25 00:00:...|       1|         CLOSED|
|              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
|            12111|2013-07-25 00:00:...|       3|       COMPLETE|
|             8827|2013-07-25 00:00:...|       4|         CLOSED|
|            11318|2013-07-25 00:00:...|       5|       COMPLETE|
|             7130|2013-07-25 00:00:...|       6|       COMPLETE|
|             4530|2013-07-25 00:00:...|       7|       COMPLETE|
|             2911|2013-07-25 00:00:...|       8|     PROCESSING|
|             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
|             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
+-----------------+--------------------+--------+---------------+

//Write data into orc format, method 2, using write.orc
val ordersDF = sqlContext.read.json("/public/retail_db_json/orders")
scala> ordersDF.write.parquet("/user/paslechoix/orders_orc2")
sqlContext.load("/user/paslechoix/orders_orc2", "orc").show(10)
Malformed ORC file

hdfs dfs -rm -R /user/paslechoix/orders_orc2

scala> ordersDF.write.orc("/user/paslechoix/orders_orc2")
//Now the data should have been saved to the right format of orc, verify on hdfs first
[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/orders_orc2
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-26 22:49 /user/paslechoix/orders_orc2/_SUCCESS
-rw-r--r--   3 paslechoix hdfs      81334 2018-01-26 22:49 /user/paslechoix/orders_orc2/part-r-00000-ca50043d-6ca2-4208-9a0a-49513c3092c2.orc
-rw-r--r--   3 paslechoix hdfs      83256 2018-01-26 22:49 /user/paslechoix/orders_orc2/part-r-00001-ca50043d-6ca2-4208-9a0a-49513c3092c2.orc

//Verify by loading it to data frame and show it
sqlContext.load("/user/paslechoix/orders_orc2", "orc").show(10)
+-----------------+--------------------+--------+---------------+
|order_customer_id|          order_date|order_id|   order_status|
+-----------------+--------------------+--------+---------------+
|            11599|2013-07-25 00:00:...|       1|         CLOSED|
|              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
|            12111|2013-07-25 00:00:...|       3|       COMPLETE|
|             8827|2013-07-25 00:00:...|       4|         CLOSED|
|            11318|2013-07-25 00:00:...|       5|       COMPLETE|
|             7130|2013-07-25 00:00:...|       6|       COMPLETE|
|             4530|2013-07-25 00:00:...|       7|       COMPLETE|
|             2911|2013-07-25 00:00:...|       8|     PROCESSING|
|             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
|             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
+-----------------+--------------------+--------+---------------+


//----------Json-----------
//Write data into json format 
ordersDF.save("/user/paslechoix/orders_json", "json")

//verify on hdfs first
[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/orders_json
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-26 22:59 /user/paslechoix/orders_json/_SUCCESS
-rw-r--r--   3 paslechoix hdfs    3738783 2018-01-26 22:59 /user/paslechoix/orders_json/part-r-00000-8de19f75-4d65-4f4b-9460-a9bbcedd7f3a
-rw-r--r--   3 paslechoix hdfs    3738556 2018-01-26 22:59 /user/paslechoix/orders_json/part-r-00001-8de19f75-4d65-4f4b-9460-a9bbcedd7f3a

//Verify by loading it to data frame and show it
sqlContext.load("/user/paslechoix/orders_json", "json").show(10)
+-----------------+--------------------+--------+---------------+
|order_customer_id|          order_date|order_id|   order_status|
+-----------------+--------------------+--------+---------------+
|            11599|2013-07-25 00:00:...|       1|         CLOSED|
|              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
|            12111|2013-07-25 00:00:...|       3|       COMPLETE|
|             8827|2013-07-25 00:00:...|       4|         CLOSED|
|            11318|2013-07-25 00:00:...|       5|       COMPLETE|
|             7130|2013-07-25 00:00:...|       6|       COMPLETE|
|             4530|2013-07-25 00:00:...|       7|       COMPLETE|
|             2911|2013-07-25 00:00:...|       8|     PROCESSING|
|             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
|             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
+-----------------+--------------------+--------+---------------+


//Comparing the size of different data format, from most compact to least: orc, parquet, json
orc:
81334 2018-01-26 22:53 /user/paslechoix/orders_orc/part-r-00000-fd2bb5f7-f24a-49e7-9813-56f15cdefbbd.orc
83256 2018-01-26 22:53 /user/paslechoix/orders_orc/part-r-00001-fd2bb5f7-f24a-49e7-9813-56f15cdefbbd.orc
parquet:
157619 2018-01-26 22:34 /user/paslechoix/orders_parquet/part-r-00000-74e456a1-4ca1-4425-bb67-316f36e53e30.gz.parquet
159746 2018-01-26 22:34 /user/paslechoix/orders_parquet/part-r-00001-74e456a1-4ca1-4425-bb67-316f36e53e30.gz.parquet
json:
3738783 2018-01-26 22:59 /user/paslechoix/orders_json/part-r-00000-8de19f75-4d65-4f4b-9460-a9bbcedd7f3a
3738556 2018-01-26 22:59 /user/paslechoix/orders_json/part-r-00001-8de19f75-4d65-4f4b-9460-a9bbcedd7f3a



//DataFrame and registered as temp table
val ordersRDD = sc.textFile("/public/retail_db/orders")
val ordersDF = ordersRDD.map(order => {
  (order.split(",")(0).toInt, order.split(",")(1).dropRight(11).replace("-",""), order.split(",")(2).toInt, order.split(",")(3))
  }).toDF("order_id", "order_date", "order_customer_id", "order_status")

+--------+----------+-----------------+---------------+
|order_id|order_date|order_customer_id|   order_status|
+--------+----------+-----------------+---------------+
|       1|  20130725|            11599|         CLOSED|
|       2|  20130725|              256|PENDING_PAYMENT|
|       3|  20130725|            12111|       COMPLETE|
|       4|  20130725|             8827|         CLOSED|
|       5|  20130725|            11318|       COMPLETE|
+--------+----------+-----------------+---------------+

ordersDF.registerTempTable("orders")
sqlContext.sql("select order_status, count(1) count_by_status from orders group by order_status").show()
+---------------+---------------+
|   order_status|count_by_status|
+---------------+---------------+
|        PENDING|           7610|
|        ON_HOLD|           3798|
| PAYMENT_REVIEW|            729|
|PENDING_PAYMENT|          15030|
|     PROCESSING|           8275|
|         CLOSED|           7556|
|       COMPLETE|          22899|
|       CANCELED|           1428|
|SUSPECTED_FRAUD|           1558|
+---------------+---------------+


sqlContext.sql("use dgadiraju_retail_db_orc")
val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList
val productsRDD = sc.parallelize(productsRaw)

val productsMap = productsRDD.map(prd => (prd.split(",")(0).toInt, prd.split(",")(2)))

val productsDF = productsRDD.map(product => {
  (product.split(",")(0).toInt, product.split(",")(2))
}).toDF("product_id", "product_name")

productsDF.registerTempTable("products")

sqlContext.sql("select * from products").show(5, truncate=false)

+----------+---------------------------------------------+
|product_id|product_name                                 |
+----------+---------------------------------------------+
|1         |Quest Q64 10 FT. x 10 FT. Slant Leg Instant U|
|2         |Under Armour Men's Highlight MC Football Clea|
|3         |Under Armour Men's Renegade D Mid Football Cl|
|4         |Under Armour Men's Renegade D Mid Football Cl|
|5         |Riddell Youth Revolution Speed Custom Footbal|
+----------+---------------------------------------------+

