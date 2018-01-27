
//Launching spark shell
spark-shell --master yarn --num-executors 2 --executor-memory 2G --conf spark.ui.port=12673

// Read orders and order_items
val orders = sc.textFile("/public/retail_db/orders")
val orderItems = sc.textFile("/public/retail_db/order_items")

orders.first
res0: String = 1,2013-07-25 00:00:00.0,11599,CLOSED

mysql> desc orders;
+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
| order_status      | varchar(45) | NO   |     | NULL    |                |
+-------------------+-------------+------+-----+---------+----------------+

orderItems.first
res0: String = 1,1,957,1,299.98,299.98

mysql> desc order_items;
+--------------------------+------------+------+-----+---------+----------------+
| Field                    | Type       | Null | Key | Default | Extra          |
+--------------------------+------------+------+-----+---------+----------------+
| order_item_id            | int(11)    | NO   | PRI | NULL    | auto_increment |
| order_item_order_id      | int(11)    | NO   |     | NULL    |                |
| order_item_product_id    | int(11)    | NO   |     | NULL    |                |
| order_item_quantity      | tinyint(4) | NO   |     | NULL    |                |
| order_item_subtotal      | float      | NO   |     | NULL    |                |
| order_item_product_price | float      | NO   |     | NULL    |                |
+--------------------------+------------+------+-----+---------+----------------+



orders.take(10).foreach(println)
orderItems.take(10).foreach(println)

// Filter for completed or closed orders
val ordersFiltered = orders.filter(order => (order.split(",")(3) == "CLOSED" || order.split(",")(3) == "COMPLETE"))

1,2013-07-25 00:00:00.0,11599,CLOSED
3,2013-07-25 00:00:00.0,12111,COMPLETE
4,2013-07-25 00:00:00.0,8827,CLOSED
5,2013-07-25 00:00:00.0,11318,COMPLETE
6,2013-07-25 00:00:00.0,7130,COMPLETE
7,2013-07-25 00:00:00.0,4530,COMPLETE
12,2013-07-25 00:00:00.0,1837,CLOSED
15,2013-07-25 00:00:00.0,2568,COMPLETE
17,2013-07-25 00:00:00.0,2667,COMPLETE
18,2013-07-25 00:00:00.0,1205,CLOSED

val ordersFiltered = orders.
  filter(order => order.split(",")(3) == "COMPLETE" || order.split(",")(3) == "CLOSED")

// Convert filtered orders to key value pair <orderId, orderDate>
val ordersMap = ordersFiltered.
  map(order => (order.split(",")(0).toInt, order.split(",")(1)))

ordersMap.take(10).foreach(println)
(1,2013-07-25 00:00:00.0)
(3,2013-07-25 00:00:00.0)
(4,2013-07-25 00:00:00.0)
(5,2013-07-25 00:00:00.0)
(6,2013-07-25 00:00:00.0)
(7,2013-07-25 00:00:00.0)
(12,2013-07-25 00:00:00.0)
(15,2013-07-25 00:00:00.0)
(17,2013-07-25 00:00:00.0)
(18,2013-07-25 00:00:00.0)
 
// Convert order_items to key value pairs <order_item_order_id, (order_item_product_id, total)> 
val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt,(oi.split(",")(2).toInt, oi.split(",")(4).toFloat)))

ordersMap.take(10).foreach(println)
orderItemsMap.take(10).foreach(println)
(1,(957,299.98))
(2,(1073,199.99))
(2,(502,250.0))
(2,(403,129.99))
(4,(897,49.98))
(4,(365,299.95))
(4,(502,150.0))
(4,(1014,199.92))
(5,(957,299.98))
(5,(365,299.95))

// As they have the same Key in <K, V>, join the two data sets by order_id 
val ordersJoin = ordersMap.join(orderItemsMap)
ordersJoin.take(10).foreach(println)
(65722,(2014-05-23 00:00:00.0,(365,119.98)))
(65722,(2014-05-23 00:00:00.0,(730,400.0)))
(65722,(2014-05-23 00:00:00.0,(1004,399.98)))
(65722,(2014-05-23 00:00:00.0,(627,199.95)))
(65722,(2014-05-23 00:00:00.0,(191,199.98)))
(23776,(2013-12-20 00:00:00.0,(1073,199.99)))
(23776,(2013-12-20 00:00:00.0,(403,129.99)))
(53926,(2014-06-30 00:00:00.0,(365,119.98)))
(53926,(2014-06-30 00:00:00.0,(191,99.99)))
(51620,(2014-06-13 00:00:00.0,(1004,399.98)))

//if apply some substring to the ordersMap, the result could be neat as below:
(65722,(2014-05-23,(365,119.98)))
(65722,(2014-05-23,(730,400.0)))
(65722,(2014-05-23,(1004,399.98)))
(65722,(2014-05-23,(627,199.95)))
(65722,(2014-05-23,(191,199.98)))
(23776,(2013-12-20,(1073,199.99)))
(23776,(2013-12-20,(403,129.99)))
(53926,(2014-06-30,(365,119.98)))
(53926,(2014-06-30,(191,99.99)))
(51620,(2014-06-13,(1004,399.98)))


Before the join:
ordersMap.count
res11: Long = 30455

orderItemsMap.count
res12: Long = 172198

After the join
ordersJoin.count
res8: Long = 75408

//Compute daily revenue
//ReduceByKey
//final output: (order_id, (order_date, (order_item_product_id, order_item_subtotal))
//daily revenue per product_id: (order_date, (order_item_product_id, order_item_subtotal))

/* ordersJoin.first 
res20: (Int, (String, (Int, Float))) = (65722,(2014-05-23,(365,119.98)))
rec._2._1: 2014-05-23
rec._2._2._1: 365
rec._2._2._2: 119.98 

What is needed for the output should be planned in advanced and then get the needed items from elements in the tuple through a map 

*/

val ordersJoinMap = ordersJoin.map(rec => ((rec._2._1, rec._2._2._1), rec._2._2._2))
res18: ((String, Int), Float) = ((2014-05-23,365),119.98)
val ordersJoinMap2 = ordersJoin.map(rec => (rec._2._1, (rec._2._2._1, rec._2._2._2)))
res19: (String, (Int, Float)) = (2014-05-23,(365,119.98))

ordersJoinMap.take(10).foreach(println)
ordersJoinMap.count

val dailyRevenuePerProductId = ordersJoinMap.
  reduceByKey((revenue, order_item_subtotal) => revenue + order_item_subtotal)
dailyRevenuePerProductId.take(10).foreach(println)
((2013-11-04,775),29.97)
((2013-08-17,235),34.99)
((2014-04-06,793),44.97)
((2014-01-15,365),7558.7393)
((2013-09-24,792),14.99)
((2014-07-12,728),260.0)
((2014-07-11,565),210.0)
((2013-12-11,116),179.96)
((2014-04-22,627),1359.6599)
((2014-07-14,957),4499.7)

//sort by order_date 
dailyRevenuePerProductId.take(10).sortBy(_._1._1).foreach(println)
((2013-08-17,235),34.99)
((2013-09-24,792),14.99)
((2013-10-13,19),124.99)
((2013-11-04,775),29.97)
((2014-01-15,365),7558.7393)
((2014-02-14,924),63.96)
((2014-04-06,793),44.97)
((2014-04-22,627),1359.6599)
((2014-07-11,565),210.0)
((2014-07-12,728),260.0)

//sort by product_id
dailyRevenuePerProductId.take(10).sortBy(_._1._2).foreach(println)
((2013-12-11,116),179.96)
((2013-08-17,235),34.99)
((2014-01-15,365),7558.7393)
((2014-07-11,565),210.0)
((2014-04-22,627),1359.6599)
((2014-07-12,728),260.0)
((2013-11-04,775),29.97)
((2013-09-24,792),14.99)
((2014-04-06,793),44.97)
((2014-07-14,957),4499.7)

//sort by revenue in desc
scala> dailyRevenuePerProductId.take(10).sortWith(_._2 > _._2).foreach(println)
((2014-01-15,365),7558.7393)
((2014-07-14,957),4499.7)
((2014-04-22,627),1359.6599)
((2014-07-12,728),260.0)
((2014-07-11,565),210.0)
((2013-12-11,116),179.96)
((2014-04-06,793),44.97)
((2013-08-17,235),34.99)
((2013-11-04,775),29.97)
((2013-09-24,792),14.99)


dailyRevenuePerProductId.count
res22: Long = 9120

