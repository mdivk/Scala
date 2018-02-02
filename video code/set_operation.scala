// Set operations

val orders = sc.textFile("/public/retail_db/orders")
val customers_201308 = orders.
  filter(order => order.split(",")(1).contains("2013-08")).
  map(order => (order.split(",")(3), order))

orders.first
res13: String = 1,2013-07-25 00:00:00.0,11599,CLOSED

scala> customers_201308.take(10).foreach(println)
(COMPLETE,1297,2013-08-01 00:00:00.0,11607,COMPLETE)
(CLOSED,1298,2013-08-01 00:00:00.0,5105,CLOSED)
(COMPLETE,1299,2013-08-01 00:00:00.0,7802,COMPLETE)
(PENDING_PAYMENT,1300,2013-08-01 00:00:00.0,553,PENDING_PAYMENT)
(PENDING_PAYMENT,1301,2013-08-01 00:00:00.0,1604,PENDING_PAYMENT)
(COMPLETE,1302,2013-08-01 00:00:00.0,1695,COMPLETE)
(PROCESSING,1303,2013-08-01 00:00:00.0,7018,PROCESSING)
(COMPLETE,1304,2013-08-01 00:00:00.0,2059,COMPLETE)
(COMPLETE,1305,2013-08-01 00:00:00.0,3844,COMPLETE)
(PENDING_PAYMENT,1306,2013-08-01 00:00:00.0,11672,PENDING_PAYMENT)

val customers201308 = orders.
  filter(order => order.split(",")(1).contains("2013-08"))
customers201308.take(10).foreach(println)
1297,2013-08-01 00:00:00.0,11607,COMPLETE
1298,2013-08-01 00:00:00.0,5105,CLOSED
1299,2013-08-01 00:00:00.0,7802,COMPLETE
1300,2013-08-01 00:00:00.0,553,PENDING_PAYMENT
1301,2013-08-01 00:00:00.0,1604,PENDING_PAYMENT
1302,2013-08-01 00:00:00.0,1695,COMPLETE
1303,2013-08-01 00:00:00.0,7018,PROCESSING
1304,2013-08-01 00:00:00.0,2059,COMPLETE
1305,2013-08-01 00:00:00.0,3844,COMPLETE
1306,2013-08-01 00:00:00.0,11672,PENDING_PAYMENT

val customers201309 = orders.
  filter(order => order.split(",")(1).contains("2013-09"))
customers201309.take(10).foreach(println)
6057,2013-09-01 00:00:00.0,437,ON_HOLD
6058,2013-09-01 00:00:00.0,9126,PROCESSING
6059,2013-09-01 00:00:00.0,11516,COMPLETE
6060,2013-09-01 00:00:00.0,8095,PROCESSING
6061,2013-09-01 00:00:00.0,7209,COMPLETE
6062,2013-09-01 00:00:00.0,1515,PENDING_PAYMENT
6063,2013-09-01 00:00:00.0,9236,CLOSED
6064,2013-09-01 00:00:00.0,10133,PENDING_PAYMENT
6065,2013-09-01 00:00:00.0,2114,COMPLETE
6066,2013-09-01 00:00:00.0,5068,COMPLETE

//Get intersection of 201308 and 201309, obviously the result would be 0
val customers_201308_and_201309 = customers201308.intersection(customers201309)
  
// Get all unique customers who placed orders in 2013 August or 2013 September
val customers_201308_union_201309 = customers_201308.union(customers_201309).distinct
scala> customers_201308_and_201309.count
res20: Long = 11521


// Get all customers who placed orders in 201308 but not in 201309, 201308 substract 201309, leftOuterJoin
val customer_201308_minus_201309 = customers201308.map(c => (c, 1)).
  leftOuterJoin(customers201309.map(c => (c, 1))).
  filter(rec => rec._2._2 == None).
  map(rec => rec._1).
  distinct