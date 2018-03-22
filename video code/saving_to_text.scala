val rdd = sc.textFile("products0317_text")
rdd.toDF.show
+--------------------+
|1,2,Quest Q64 10 ...|
|2,2,Under Armour ...|
|3,2,Under Armour ...|
|4,2,Under Armour ...|
|5,2,Riddell Youth...|
|6,2,Jordan Men's ...|
|7,2,Schutt Youth ...|
|8,2,Nike Men's Va...|
|9,2,Nike Adult Va...|
|10,2,Under Armour...|
|11,2,Fitness Gear...|
|12,2,Under Armour...|
|13,2,Under Armour...|
|14,2,Quik Shade S...|
|15,2,Under Armour...|
|16,2,Riddell Yout...|
|17,2,Under Armour...|
|18,2,Reebok Men's...|
|19,2,Nike Men's F...|
|20,2,Under Armour...|
+--------------------+

write textFile

rdd.saveAsTextFile("products0321_text")

compression

scala> sc.textFile("products0317_text").saveAsTextFile("products0321_text_compressed", classOf[org.apache.hadoop.io.compress.SnappyCodec])

[paslechoix@gw03 ~]$ hdfs dfs -ls products0321_text_compressed
Found 5 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-21 21:38 products0321_text_compressed/_SUCCESS
-rw-r--r--   3 paslechoix hdfs      11197 2018-03-21 21:38 products0321_text_compressed/part-00000.snappy
-rw-r--r--   3 paslechoix hdfs      11033 2018-03-21 21:38 products0321_text_compressed/part-00001.snappy
-rw-r--r--   3 paslechoix hdfs      12255 2018-03-21 21:38 products0321_text_compressed/part-00002.snappy
-rw-r--r--   3 paslechoix hdfs      13354 2018-03-21 21:38 products0321_text_compressed/part-00003.snappy
[paslechoix@gw03 ~]$
