val df = sqlContext.read.parquet("orders0314_par")
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


with compress - gzip: 
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
df.write.parquet("orders0314_par_gzip_output")

hdfs dfs -ls orders0314_par_gzip_output

[paslechoix@gw03 data]$ hdfs dfs -ls orders0314_par_output
Found 7 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-20 07:10 orders0314_par_output/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        497 2018-03-20 07:10 orders0314_par_output/_common_metadata
-rw-r--r--   3 paslechoix hdfs       2603 2018-03-20 07:10 orders0314_par_output/_metadata
-rw-r--r--   3 paslechoix hdfs      85098 2018-03-20 07:10 orders0314_par_output/part-r-00000-31813a69-1659-4bab-9941-44192a95e475.gz.parquet
-rw-r--r--   3 paslechoix hdfs      85076 2018-03-20 07:10 orders0314_par_output/part-r-00001-31813a69-1659-4bab-9941-44192a95e475.gz.parquet
-rw-r--r--   3 paslechoix hdfs      84964 2018-03-20 07:10 orders0314_par_output/part-r-00002-31813a69-1659-4bab-9941-44192a95e475.gz.parquet
-rw-r--r--   3 paslechoix hdfs      88275 2018-03-20 07:10 orders0314_par_output/part-r-00003-31813a69-1659-4bab-9941-44192a95e475.gz.parquet

with compress - snappy: 

sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
df.write.parquet("orders0314_par_snappy_output")

hdfs dfs -ls orders0314_par_snappy_output

[paslechoix@gw03 data]$ hdfs dfs -ls orders0314_par_snappy_output
Found 7 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-20 07:17 orders0314_par_snappy_output/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        497 2018-03-20 07:17 orders0314_par_snappy_output/_common_metadata
-rw-r--r--   3 paslechoix hdfs       2671 2018-03-20 07:17 orders0314_par_snappy_output/_metadata
-rw-r--r--   3 paslechoix hdfs     147257 2018-03-20 07:17 orders0314_par_snappy_output/part-r-00000-ec8617d2-86b7-4c2d-9664-5039b77093cc.snappy.parquet
-rw-r--r--   3 paslechoix hdfs     147097 2018-03-20 07:17 orders0314_par_snappy_output/part-r-00001-ec8617d2-86b7-4c2d-9664-5039b77093cc.snappy.parquet
-rw-r--r--   3 paslechoix hdfs     147055 2018-03-20 07:17 orders0314_par_snappy_output/part-r-00002-ec8617d2-86b7-4c2d-9664-5039b77093cc.snappy.parquet
-rw-r--r--   3 paslechoix hdfs     151532 2018-03-20 07:17 orders0314_par_snappy_output/part-r-00003-ec8617d2-86b7-4c2d-9664-5039b77093cc.snappy.parquet
