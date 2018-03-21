spark-shell --packages com.databricks:spark-avro_2.10:2.0.1
val orders = sc.textFile("orders03121")
res1: String = 1,CLOSED

val ordersDF = orders.map(x => (x.split(",")(0).toInt, x.split(",")(1))).toDF("order_id", "order_status")
+--------+---------------+
|order_id|   order_status|
+--------+---------------+
|       1|         CLOSED|
|       2|PENDING_PAYMENT|
|       3|       COMPLETE|
|       4|         CLOSED|
|       5|       COMPLETE|
|       6|       COMPLETE|
|       7|       COMPLETE|
|       8|     PROCESSING|
|       9|PENDING_PAYMENT|
|      10|PENDING_PAYMENT|
|      11| PAYMENT_REVIEW|
|      12|         CLOSED|
|      13|PENDING_PAYMENT|
|      14|     PROCESSING|
|      15|       COMPLETE|
|      16|PENDING_PAYMENT|
|      17|       COMPLETE|
|      18|         CLOSED|
|      19|PENDING_PAYMENT|
|      20|     PROCESSING|
+--------+---------------+
only showing top 20 rows


import com.databricks.spark.avro._

sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
ordersDF.write.avro("orders_result_snappy")

hdfs dfs -ls -h orders_result_snappy
[paslechoix@gw03 data]$ hdfs dfs -ls -h orders_result_snappy
Found 13 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-20 06:59 orders_result_snappy/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     85.2 K 2018-03-20 06:59 orders_result_snappy/part-r-00000-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     87.6 K 2018-03-20 06:59 orders_result_snappy/part-r-00001-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     87.9 K 2018-03-20 06:59 orders_result_snappy/part-r-00002-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     87.8 K 2018-03-20 06:59 orders_result_snappy/part-r-00003-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     85.2 K 2018-03-20 06:59 orders_result_snappy/part-r-00004-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     87.6 K 2018-03-20 06:59 orders_result_snappy/part-r-00005-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     87.9 K 2018-03-20 06:59 orders_result_snappy/part-r-00006-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     87.8 K 2018-03-20 06:59 orders_result_snappy/part-r-00007-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     85.4 K 2018-03-20 06:59 orders_result_snappy/part-r-00008-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     85.4 K 2018-03-20 06:59 orders_result_snappy/part-r-00009-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     85.4 K 2018-03-20 06:59 orders_result_snappy/part-r-00010-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro
-rw-r--r--   3 paslechoix hdfs     88.6 K 2018-03-20 06:59 orders_result_snappy/part-r-00011-05162ead-4a5a-4688-9bc5-fac17460b4fb.avro

sqlContext.read.avro("orders_result_snappy").show(10)
+--------+---------------+
|order_id|   order_status|
+--------+---------------+
|       1|         CLOSED|
|       2|PENDING_PAYMENT|
|       3|       COMPLETE|
|       4|         CLOSED|
|       5|       COMPLETE|
|       6|       COMPLETE|
|       7|       COMPLETE|
|       8|     PROCESSING|
|       9|PENDING_PAYMENT|
|      10|PENDING_PAYMENT|
+--------+---------------+
only showing top 10 rows
