val ordersDF = sqlContext.read.json("/public/retail_db_json/orders")

avro:

import com.databricks.spark.avro._
avro:

ordersDF.write.avro("/user/paslechoix/orders_avro")

Text:
ordersDF.rdd.saveAsTextFile("/user/paslechoix/orders_text")


sequence:

Sequence file cannot be generated via DF.rdd, it can be generated via rdd though.

val orders = sc.textFile("/public/retail_db/orders")
orders.map(rec => (NullWritable.get(), rec)).saveAsSequenceFile("/user/paslechoix/orders_seq")

//Comparing the size of different data format, from most compact to least: orc, sequence, parquet, avro, text, json

orc:
81334 2018-01-26 22:53 /user/paslechoix/orders_orc/part-r-00000-fd2bb5f7-f24a-49e7-9813-56f15cdefbbd.orc
83256 2018-01-26 22:53 /user/paslechoix/orders_orc/part-r-00001-fd2bb5f7-f24a-49e7-9813-56f15cdefbbd.orc

sequence:
94038 2018-03-25 18:02 /user/paslechoix/orders_seq/part-00000
93544 2018-03-25 18:02 /user/paslechoix/orders_seq/part-00001

parquet:
157619 2018-01-26 22:34 /user/paslechoix/orders_parquet/part-r-00000-74e456a1-4ca1-4425-bb67-316f36e53e30.gz.parquet
159746 2018-01-26 22:34 /user/paslechoix/orders_parquet/part-r-00001-74e456a1-4ca1-4425-bb67-316f36e53e30.gz.parquet

avro:
411010 2018-03-25 17:45 /user/paslechoix/orders_avro/part-r-00000-f38c5c32-f7f8-430e-b561-72fa5fc5b01a.avro
424676 2018-03-25 17:45 /user/paslechoix/orders_avro/part-r-00001-f38c5c32-f7f8-430e-b561-72fa5fc5b01a.avro

text:
1565787 2018-03-25 17:48 /user/paslechoix/orders_text/part-00000
1571923 2018-03-25 17:48 /user/paslechoix/orders_text/part-00001

json:
3738783 2018-01-26 22:59 /user/paslechoix/orders_json/part-r-00000-8de19f75-4d65-4f4b-9460-a9bbcedd7f3a
3738556 2018-01-26 22:59 /user/paslechoix/orders_json/part-r-00001-8de19f75-4d65-4f4b-9460-a9bbcedd7f3a