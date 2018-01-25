RDD

RDD to preview data

doc: https://spark.apache.org/docs/1.6.3/programming-guide.html

1. Start cygwin
2. login the lab: ssh paslechoix@gw01.itcversity.com 
since passwordless was set up already, no password is asked here.

3. start spark-shell (for scala) or pyspark (for python)
spark-shell --master yarn --conf spark.ui.port=12654 --num-executors 1 --executor-memory 1024M

Transformation

To use external command inside scala shell, import the sys.process first

scala> import sys.process._
import sys.process._

Now run the external commands as below:

scala> "ls -ltr" !

scala> "hdfs dfs -ls /public/retail_db/orders"!
warning: there were 1 feature warning(s); re-run with -feature for details
Found 1 items
-rw-r--r--   3 hdfs hdfs    2999944 2016-12-19 03:52 /public/retail_db/orders/part-00000
res1: Int = 0


val orders = sc.textFile("/public/retail_db/orders")

scala> orders.take(10).foreach(println)
1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
3,2013-07-25 00:00:00.0,12111,COMPLETE
4,2013-07-25 00:00:00.0,8827,CLOSED
5,2013-07-25 00:00:00.0,11318,COMPLETE
6,2013-07-25 00:00:00.0,7130,COMPLETE
7,2013-07-25 00:00:00.0,4530,COMPLETE
8,2013-07-25 00:00:00.0,2911,PROCESSING
9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT
10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT


val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList

val products = sc.parallelize(productsRaw)

Action