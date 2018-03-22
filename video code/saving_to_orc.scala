hiveContext.sql("select * form orders limit 10").write.orc("orders0320_orc")

import org.apache.hadoop.io.LongWritable
val data = sc.sequenceFile[LongWritable,orders]("orders03132_seq")
data.map(tup => (tup._1.get(), tup._2.toString())).collect.foreach(println)

case class Orders(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)

val seqRDD=sc.sequenceFile("orders03132_seq",classOf[org.apache.hadoop.io.Text],classOf[org.apache.hadoop.io.Text])

val rdd = sc.textFile("")

val df = sqlContext.read.parquet("orders03132_seq")
+--------+-------------+-----------------+---------------+
|order_id|   order_date|order_customer_id|   order_status|
+--------+-------------+-----------------+---------------+
|   34443|1393045200000|             2742|         CLOSED|
|   34444|1393045200000|             9487|       COMPLETE|
|   34445|1393045200000|             2391|        ON_HOLD|
|   34446|1393045200000|             9912|         CLOSED|
|   34447|1393045200000|             2826|     PROCESSING|
|   34448|1393045200000|             7155|       CANCELED|
|   34449|1393045200000|             5724|     PROCESSING|
|   34450|1393045200000|             7992|       COMPLETE|
|   34451|1393045200000|            11255|       COMPLETE|
|   34452|1393045200000|             8512|        PENDING|
|   34453|1393045200000|             9582|       COMPLETE|
|   34454|1393045200000|             2020|       CANCELED|
|   34455|1393045200000|             4983|PENDING_PAYMENT|
|   34456|1393045200000|             8538|       COMPLETE|
|   34457|1393045200000|             3164|PENDING_PAYMENT|
|   34458|1393045200000|             7964|        ON_HOLD|
|   34459|1393045200000|             3995|       COMPLETE|
|   34460|1393045200000|             2653|       COMPLETE|
|   34461|1393045200000|             2220|SUSPECTED_FRAUD|
|   34462|1393045200000|              392|       COMPLETE|
+--------+-------------+-----------------+---------------+

df.write.parquet("orders0314_par_output")

[paslechoix@gw03 data]$ hdfs dfs -ls orders0314_par_output
Found 7 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-20 07:10 orders0314_par_output/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        497 2018-03-20 07:10 orders0314_par_output/_common_metadata
-rw-r--r--   3 paslechoix hdfs       2603 2018-03-20 07:10 orders0314_par_output/_metadata
-rw-r--r--   3 paslechoix hdfs      85098 2018-03-20 07:10 orders0314_par_output/part-r-00000-31813a69-1659-4bab-9941-44192a95e475.gz.parquet
-rw-r--r--   3 paslechoix hdfs      85076 2018-03-20 07:10 orders0314_par_output/part-r-00001-31813a69-1659-4bab-9941-44192a95e475.gz.parquet
-rw-r--r--   3 paslechoix hdfs      84964 2018-03-20 07:10 orders0314_par_output/part-r-00002-31813a69-1659-4bab-9941-44192a95e475.gz.parquet
-rw-r--r--   3 paslechoix hdfs      88275 2018-03-20 07:10 orders0314_par_output/part-r-00003-31813a69-1659-4bab-9941-44192a95e475.gz.parquet

re-read out:

sqlContext.read.parquet("orders0314_par_output").show 

+--------+-------------+-----------------+---------------+
|order_id|   order_date|order_customer_id|   order_status|
+--------+-------------+-----------------+---------------+
|   34443|1393045200000|             2742|         CLOSED|
|   34444|1393045200000|             9487|       COMPLETE|
|   34445|1393045200000|             2391|        ON_HOLD|
|   34446|1393045200000|             9912|         CLOSED|
|   34447|1393045200000|             2826|     PROCESSING|
|   34448|1393045200000|             7155|       CANCELED|
|   34449|1393045200000|             5724|     PROCESSING|
|   34450|1393045200000|             7992|       COMPLETE|
|   34451|1393045200000|            11255|       COMPLETE|
|   34452|1393045200000|             8512|        PENDING|
|   34453|1393045200000|             9582|       COMPLETE|
|   34454|1393045200000|             2020|       CANCELED|
|   34455|1393045200000|             4983|PENDING_PAYMENT|
|   34456|1393045200000|             8538|       COMPLETE|
|   34457|1393045200000|             3164|PENDING_PAYMENT|
|   34458|1393045200000|             7964|        ON_HOLD|
|   34459|1393045200000|             3995|       COMPLETE|
|   34460|1393045200000|             2653|       COMPLETE|
|   34461|1393045200000|             2220|SUSPECTED_FRAUD|
|   34462|1393045200000|              392|       COMPLETE|
+--------+-------------+-----------------+---------------+
only showing top 20 rows
