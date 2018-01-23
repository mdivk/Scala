Transformation

String manipulation

val orders = sc.textFile("/public/retail_db/orders")
val order = orders.first
order.split(',')
scala> order.split(',')
res8: Array[String] = Array(1, 2013-07-25 00:00:00.0, 11599, CLOSED)

to access element in an array:
in scala, index starts with 0

scala> order.split(',')(0)
1
scala> order.split(',')(1)
res9: String = 2013-07-25 00:00:00.0

scala> val str = order.split(',')
str: Array[String] = Array(1, 2013-07-25 00:00:00.0, 11599, CLOSED)

scala> str.contains("2018")
res11: Boolean = false

scala> val orderDate = str(1)
orderDate: String = 2013-07-25 00:00:00.0

scala> val year = orderDate.substring(0,4)
year: String = 2013

scala> orderDate.replace('-', '/')
res13: String = 2013/07/25 00:00:00.0

scala> orderDate.indexOf("25")
res14: Int = 8

scala> val orderDateTransformed=orderDate.replace("-", "").substring(0,8).toInt
orderDateTransformed: Int = 20130725


String functions:
scala> orderDate.
+                     asInstanceOf          charAt                chars                 codePointAt
codePointBefore       codePointCount        codePoints            compareTo             compareToIgnoreCase
concat                contains              contentEquals         endsWith              equalsIgnoreCase
getBytes              getChars              indexOf               intern                isEmpty
isInstanceOf          lastIndexOf           length                matches               offsetByCodePoints
regionMatches         replace               replaceAll            replaceFirst          split
startsWith            subSequence           substring             toCharArray           toLowerCase
toString              toUpperCase           trim


Filtering

Row level transformation
map

Using map function for rdd, convert the whole string to only the second column (after some transformation)

scala> val orderDateMapped = orders.map((order:String)=>{order.split(",")(1).substring(0,10).replace("-", "").toInt})

scala> orderDateMapped.first
res4: Int = 20130725


flatmap
// Row level transformations using flatMap

val l = List("Hello", "How are you doing", "Let us perform word count", "As part of the word count program", "we will see how many times each word repeat")
val l_rdd = sc.parallelize(l)
val l_map = l_rdd.map(ele => ele.split(" "))
val l_flatMap = l_rdd.flatMap(ele => ele.split(" "))
val wordcount = l_flatMap.map(word => (word, "")).countByKey

scala> l
res18: List[String] = List(Hello, How are you doing, Let us perform word count, As part of the word count program, we will see how many times each word repeat)

scala> l_rdd.take(10)
res17: Array[String] = Array(Hello, How are you doing, Let us perform word count, As part of the word count program, we will see how many times each word repeat)

scala> l_map.take(10)
res16: Array[Array[String]] = Array(Array(Hello), Array(How, are, you, doing), Array(Let, us, perform, word, count), Array(As, part, of, the, word, count, program), Array(we, will, see, how, many, times, each, word, repeat))

scala> l_flatMap.take(10)
res19: Array[String] = Array(Hello, How, are, you, doing, Let, us, perform, word, count)


Joins

Aggregation


val orders = sc.textFile("/public/retail_db/orders")
val str = orders.first
str.split(",")(1).substring(0, 10).replace("-", "").toInt
var orderDates = orders.map ((str: String) => {
	str.split(",")(1).substring(0, 10).replace("-", "").toInt
})

val ordersPairedRDD = orders.map(order=>{
	val o = order.split(",")
	(o(0).toInt, o(1).substring(0,10).replace("-","").toInt, o(2))
})

scala> ordersPairedRDD.take(10).foreach(println)
(1,20130725)
(2,20130725)
(3,20130725)
(4,20130725)
(5,20130725)
(6,20130725)
(7,20130725)
(8,20130725)
(9,20130725)
(10,20130725)

Test the following and not successful:

val firstOrder = orders.first
val one = firstOrder.split(",")(one(0).toInt,one(1).substring(0,10).replace("-","").toInt
val one = firstOrder.split(",")(one(0).toInt, one(1).substring(0, 10).replace("-", "").toInt)


val orderItems = sc.textFile("/public/retail_db/order_items")
val orderItemsPairedRDD = orderItems.map(orderItem => {
	(orderItem.split(",")(1).toInt, orderItem)
})	

orders.take(10).foreach(println)


scala> orderItemsPairedRDD.take(10).foreach(println)
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

val orderItemsPairedRDD0 = orderItems.map(orderItem => {
	(orderItem.split(",")(0).toInt, orderItem)
})

scala> orderItemsPairedRDD0.take(10).foreach(println)
(1,1,1,957,1,299.98,299.98)
(2,2,2,1073,1,199.99,199.99)
(3,3,2,502,5,250.0,50.0)
(4,4,2,403,1,129.99,129.99)
(5,5,4,897,2,49.98,24.99)
(6,6,4,365,5,299.95,59.99)
(7,7,4,502,3,150.0,50.0)
(8,8,4,1014,4,199.92,49.98)
(9,9,5,957,1,299.98,299.98)
(10,10,5,365,5,299.95,59.99)

val orderItemsPairedRDD1 = orderItems.map(orderItem => {
	(orderItem.split(",")(1).toInt, orderItem)
})

scala> orderItemsPairedRDD1.take(10).foreach(println)
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


val orderItemsPairedRDD2 = orderItems.map(orderItem => {
	(orderItem.split(",")(2).toInt, orderItem)
})

scala> orderItemsPairedRDD2.take(10).foreach(println)
(957,1,1,957,1,299.98,299.98)
(1073,2,2,1073,1,199.99,199.99)
(502,3,2,502,5,250.0,50.0)
(403,4,2,403,1,129.99,129.99)
(897,5,4,897,2,49.98,24.99)
(365,6,4,365,5,299.95,59.99)
(502,7,4,502,3,150.0,50.0)
(1014,8,4,1014,4,199.92,49.98)
(957,9,5,957,1,299.98,299.98)
(365,10,5,365,5,299.95,59.99)

Because the common field is orderId, orderId is RDD(0) in orders and RDD(1) in orderItems, so we need to use 
val orderItemsPairedRDD1 = orderItems.map(orderItem => {
	(orderItem.split(",")(1).toInt, orderItem)
})

scala> orderItemsPairedRDD1.take(10).foreach(println)
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

RDD(0): orders: orderId				RDD(1): orderItems: orderItem_orderId								
1,2013-07-25 00:00:00.0,11599,CLOSED              1,1,957,1,299.98,299.98
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT       2,2,1073,1,199.99,199.99
3,2013-07-25 00:00:00.0,12111,COMPLETE            3,2,502,5,250.0,50.0
4,2013-07-25 00:00:00.0,8827,CLOSED               4,2,403,1,129.99,129.99
5,2013-07-25 00:00:00.0,11318,COMPLETE            5,4,897,2,49.98,24.99
6,2013-07-25 00:00:00.0,7130,COMPLETE             6,4,365,5,299.95,59.99
7,2013-07-25 00:00:00.0,4530,COMPLETE             7,4,502,3,150.0,50.0
8,2013-07-25 00:00:00.0,2911,PROCESSING           8,4,1014,4,199.92,49.98
9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT      9,5,957,1,299.98,299.98
10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT     10,5,365,5,299.95,59.99

After Join:

1,2013-07-25 00:00:00.0,11599,CLOSED              1,1,957,1,299.98,299.98
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT       2,2,1073,1,199.99,199.99
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT       3,2,502,5,250.0,50.0
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT 	  4,2,403,1,129.99,129.99
4,2013-07-25 00:00:00.0,8827,CLOSED               5,4,897,2,49.98,24.99	 
4,2013-07-25 00:00:00.0,8827,CLOSED               6,4,365,5,299.95,59.99
4,2013-07-25 00:00:00.0,8827,CLOSED               7,4,502,3,150.0,50.0
4,2013-07-25 00:00:00.0,8827,CLOSED               8,4,1014,4,199.92,49.98
5,2013-07-25 00:00:00.0,11318,COMPLETE            9,5,957,1,299.98,299.98
5,2013-07-25 00:00:00.0,11318,COMPLETE            10,5,365,5,299.95,59.99

This type of transformation could be and should be handled in DataFrame by registering the DF to a temp table and then query with SparkSQL


