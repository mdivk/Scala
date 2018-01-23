// Joining orders and order_items ---- Inner Join
val orders = sc.textFile("/public/retail_db/orders")

scala> orders.take(1)
res0: Array[String] = Array(1,2013-07-25 00:00:00.0,11599,CLOSED)

val orderItems = sc.textFile("/public/retail_db/order_items")

scala> orderItems.take(1)
res1: Array[String] = Array(1,1,957,1,299.98,299.98)


val ordersMap = orders.map(order => {
  (order.split(",")(0).toInt, order.split(",")(1).substring(0, 10))
})

scala> ordersMap.take(1)
res2: Array[(Int, String)] = Array((1,2013-07-25))

val orderItemsMap = orderItems.map(orderItem => {
  val oi = orderItem.split(",")
  (oi(1).toInt, oi(4).toFloat)
})

scala> orderItemsMap.take(1)
res3: Array[(Int, Float)] = Array((1,299.98))

val ordersJoin = ordersMap.join(orderItemsMap)

After join, the new rdd result shows the (common key, (value_first_rdd, value_second_rdd))

scala> ordersJoin.take(10).foreach(println)
(41234,(2014-04-04,109.94))
(65722,(2014-05-23,119.98))
(65722,(2014-05-23,400.0))
(65722,(2014-05-23,399.98))
(65722,(2014-05-23,199.95))
(65722,(2014-05-23,199.98))
(28730,(2014-01-18,299.95))
(28730,(2014-01-18,50.0))
(68522,(2014-06-05,329.99))
(23776,(2013-12-20,199.99))

// Joining orders and order_items ---- Outer Join
// Get all the orders which do not have corresponding entries in order items
val orders = sc.textFile("/public/retail_db/orders")
val orderItems = sc.textFile("/public/retail_db/order_items")
val ordersMap = orders.map(order => {
  (order.split(",")(0).toInt, order)
})
scala> ordersMap.take(10).foreach(println)
(1,1,2013-07-25 00:00:00.0,11599,CLOSED)
(2,2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT)
(3,3,2013-07-25 00:00:00.0,12111,COMPLETE)
(4,4,2013-07-25 00:00:00.0,8827,CLOSED)
(5,5,2013-07-25 00:00:00.0,11318,COMPLETE)
(6,6,2013-07-25 00:00:00.0,7130,COMPLETE)
(7,7,2013-07-25 00:00:00.0,4530,COMPLETE)
(8,8,2013-07-25 00:00:00.0,2911,PROCESSING)
(9,9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT)
(10,10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT)


val ordersMap2 = orders.map(order => {
  (order.split(",")(0).toInt, order.split(",")(1).substring(0, 10))
})

scala> ordersMap2.take(10).foreach(println)
(1,2013-07-25)
(2,2013-07-25)
(3,2013-07-25)
(4,2013-07-25)
(5,2013-07-25)
(6,2013-07-25)
(7,2013-07-25)
(8,2013-07-25)
(9,2013-07-25)
(10,2013-07-25)

val orderItems = sc.textFile("/public/retail_db/order_items")

scala> orderItems.take(10).foreach(println)
1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
5,4,897,2,49.98,24.99
6,4,365,5,299.95,59.99
7,4,502,3,150.0,50.0
8,4,1014,4,199.92,49.98
9,5,957,1,299.98,299.98
10,5,365,5,299.95,59.99


Get the orderId and use it as the key in the new RDD orderItemsMap:
val orderItemsMap = orderItems.map(orderItem => {
  val oi = orderItem.split(",")
  (oi(1).toInt, orderItem)
})

scala> orderItemsMap.take(10).foreach(println)
(1,1,1,957,1,299.98,299.98)
(2,2,2,1073,1,199.99,199.99)
(2,3,2,502,5,250.0,50.0)
(2,4,2,403,1,129.99,129.99)
(4,5,4,897,2,49.98,24.99)
(4,6,4,365,5,299.95,59.99)
(4,7,4,502,3,150.0,50.0)
(4,8,4,1014,4,199.92,49.98)
(5,9,5,957,1,299.98,299.98)
(5,10,5,365,5,299.95,59.99)

val ordersLeftOuterJoin = ordersMap.leftOuterJoin(orderItemsMap)
ordersLeftOuterJoin.take(10).foreach(println)
(41234,(41234,2014-04-04 00:00:00.0,3182,PENDING_PAYMENT,Some(102921,41234,249,2,109.94,54.97)))
(65722,(65722,2014-05-23 00:00:00.0,4077,COMPLETE,Some(164249,65722,365,2,119.98,59.99)))
(65722,(65722,2014-05-23 00:00:00.0,4077,COMPLETE,Some(164250,65722,730,5,400.0,80.0)))
(65722,(65722,2014-05-23 00:00:00.0,4077,COMPLETE,Some(164251,65722,1004,1,399.98,399.98)))
(65722,(65722,2014-05-23 00:00:00.0,4077,COMPLETE,Some(164252,65722,627,5,199.95,39.99)))
(65722,(65722,2014-05-23 00:00:00.0,4077,COMPLETE,Some(164253,65722,191,2,199.98,99.99)))
(28730,(28730,2014-01-18 00:00:00.0,6069,PENDING_PAYMENT,Some(71921,28730,365,5,299.95,59.99)))
(28730,(28730,2014-01-18 00:00:00.0,6069,PENDING_PAYMENT,Some(71922,28730,502,1,50.0,50.0)))
(68522,(68522,2014-06-05 00:00:00.0,880,SUSPECTED_FRAUD,Some(171323,68522,127,1,329.99,329.99)))
(23776,(23776,2013-12-20 00:00:00.0,4041,COMPLETE,Some(59498,23776,1073,1,199.99,199.99)))


val ordersLeftOuterJoinFilter = ordersLeftOuterJoin.filter(order => order._2._2 == None)
ordersLeftOuterJoinFilter.take(10).foreach(println)
(5354,(5354,2013-08-26 00:00:00.0,7616,PENDING_PAYMENT,None))
(40888,(40888,2014-04-02 00:00:00.0,4528,CLOSED,None))
(62490,(62490,2014-01-22 00:00:00.0,8942,ON_HOLD,None))
(63508,(63508,2014-02-28 00:00:00.0,1607,COMPLETE,None))
(37370,(37370,2014-03-12 00:00:00.0,10541,COMPLETE,None))
(12420,(12420,2013-10-09 00:00:00.0,449,PENDING,None))
(1732,(1732,2013-08-03 00:00:00.0,2851,PENDING_PAYMENT,None))
(1550,(1550,2013-08-02 00:00:00.0,3043,PENDING_PAYMENT,None))
(2938,(2938,2013-08-10 00:00:00.0,116,COMPLETE,None))
(21834,(21834,2013-12-06 00:00:00.0,12334,COMPLETE,None))


val ordersWithNoOrderItem = ordersLeftOuterJoinFilter.map(order => order._2._1)
ordersWithNoOrderItem.take(10).foreach(println)
5354,2013-08-26 00:00:00.0,7616,PENDING_PAYMENT
40888,2014-04-02 00:00:00.0,4528,CLOSED
62490,2014-01-22 00:00:00.0,8942,ON_HOLD
63508,2014-02-28 00:00:00.0,1607,COMPLETE
37370,2014-03-12 00:00:00.0,10541,COMPLETE
12420,2013-10-09 00:00:00.0,449,PENDING
1732,2013-08-03 00:00:00.0,2851,PENDING_PAYMENT
1550,2013-08-02 00:00:00.0,3043,PENDING_PAYMENT
2938,2013-08-10 00:00:00.0,116,COMPLETE
21834,2013-12-06 00:00:00.0,12334,COMPLETE


val ordersRightOuterJoin = orderItemsMap.rightOuterJoin(ordersMap)
ordersRightOuterJoin.take(10).foreach(println)
(41234,(Some(102921,41234,249,2,109.94,54.97),41234,2014-04-04 00:00:00.0,3182,PENDING_PAYMENT))
(65722,(Some(164249,65722,365,2,119.98,59.99),65722,2014-05-23 00:00:00.0,4077,COMPLETE))
(65722,(Some(164250,65722,730,5,400.0,80.0),65722,2014-05-23 00:00:00.0,4077,COMPLETE))
(65722,(Some(164251,65722,1004,1,399.98,399.98),65722,2014-05-23 00:00:00.0,4077,COMPLETE))
(65722,(Some(164252,65722,627,5,199.95,39.99),65722,2014-05-23 00:00:00.0,4077,COMPLETE))
(65722,(Some(164253,65722,191,2,199.98,99.99),65722,2014-05-23 00:00:00.0,4077,COMPLETE))
(28730,(Some(71921,28730,365,5,299.95,59.99),28730,2014-01-18 00:00:00.0,6069,PENDING_PAYMENT))
(28730,(Some(71922,28730,502,1,50.0,50.0),28730,2014-01-18 00:00:00.0,6069,PENDING_PAYMENT))
(68522,(Some(171323,68522,127,1,329.99,329.99),68522,2014-06-05 00:00:00.0,880,SUSPECTED_FRAUD))
(23776,(Some(59498,23776,1073,1,199.99,199.99),23776,2013-12-20 00:00:00.0,4041,COMPLETE))


val ordersWithNoOrderItem = ordersRightOuterJoin.
  filter(order => order._2._1 == None).
  map(order => order._2._2)
ordersWithNoOrderItem.take(10).foreach(println)
5354,2013-08-26 00:00:00.0,7616,PENDING_PAYMENT
40888,2014-04-02 00:00:00.0,4528,CLOSED
62490,2014-01-22 00:00:00.0,8942,ON_HOLD
63508,2014-02-28 00:00:00.0,1607,COMPLETE
37370,2014-03-12 00:00:00.0,10541,COMPLETE
12420,2013-10-09 00:00:00.0,449,PENDING
1732,2013-08-03 00:00:00.0,2851,PENDING_PAYMENT
1550,2013-08-02 00:00:00.0,3043,PENDING_PAYMENT
21834,2013-12-06 00:00:00.0,12334,COMPLETE
2938,2013-08-10 00:00:00.0,116,COMPLETE

