//Saving the RDD to HDFS
orderCountByStatus.saveAsTextFile("/user/paslechoix/orderCountByStatus")

//verify the result in HDFS by read it out again and print it, and it shows same data 
sc.textFile("/user/paslechoix/orderCountByStatus").collect.foreach(println)
PENDING_PAYMENT 15030
CLOSED  7556
CANCELED        1428
PAYMENT_REVIEW  729
PENDING 7610
ON_HOLD 3798
PROCESSING      8275
SUSPECTED_FRAUD 1558
COMPLETE        22899

//verify it directly on HDFS
[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/orderCountByStatus
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-26 06:29 /user/paslechoix/orderCountByStatus/_SUCCESS
-rw-r--r--   3 paslechoix hdfs         34 2018-01-26 06:29 /user/paslechoix/orderCountByStatus/part-00000
-rw-r--r--   3 paslechoix hdfs        111 2018-01-26 06:29 /user/paslechoix/orderCountByStatus/part-00001

//inspect the content on HDFS
[paslechoix@gw01 ~]$ hdfs dfs -cat /user/paslechoix/orderCountByStatus/part*
PENDING_PAYMENT 15030
CLOSED  7556
CANCELED        1428
PAYMENT_REVIEW  729
PENDING 7610
ON_HOLD 3798
PROCESSING      8275
SUSPECTED_FRAUD 1558
COMPLETE        22899



