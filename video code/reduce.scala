Save RDD to HDFS

val orders = sc.textFile("/public/retail_db/orders")
res0: String = 1,2013-07-25 00:00:00.0,11599,CLOSED

//using reduceByKey to get the total for each category and sort by it
val orderByStatus = orders.map(order=>(order.split(",")(3), 1)).reduceByKey((total, element) => total + element).sortBy(_._1)
(CANCELED,1428)
(CLOSED,7556)
(COMPLETE,22899)
(ON_HOLD,3798)
(PAYMENT_REVIEW,729)
(PENDING,7610)
(PENDING_PAYMENT,15030)
(PROCESSING,8275)
(SUSPECTED_FRAUD,1558)

//a different map to transform the result into key <tab> value
val orderCountByStatus = orderByStatus.map(rec => rec._1 + "\t" + rec._2)
orderCountByStatus.collect.foreach(println)
PENDING_PAYMENT 15030
CLOSED  7556
CANCELED        1428
PAYMENT_REVIEW  729
PENDING 7610
ON_HOLD 3798
PROCESSING      8275
SUSPECTED_FRAUD 1558
COMPLETE        22899

