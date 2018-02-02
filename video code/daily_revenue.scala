//Compute daily revenue
//ReduceByKey
//final output: (order_id, (order_date, (order_item_product_id, order_item_subtotal))
//daily revenue per product_id: (order_date, (order_item_product_id, order_item_subtotal))
//interact with data from local file system
// Load products from local file system and convert into RDD /data/retail_db/products/part-00000

import scala.io.Source
val productsRaw = Source.
  fromFile("/data/retail_db/products/part-00000").
  getLines.
  toList
//By parallelize the list to transform it into a RDD 
val productsRDD = sc.parallelize(productsRaw)
res0: String = 1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy
mysql> desc products;
+---------------------+--------------+------+-----+---------+----------------+
| Field               | Type         | Null | Key | Default | Extra          |
+---------------------+--------------+------+-----+---------+----------------+
| product_id          | int(11)      | NO   | PRI | NULL    | auto_increment |
| product_category_id | int(11)      | NO   |     | NULL    |                |
| product_name        | varchar(45)  | NO   |     | NULL    |                |
| product_description | varchar(255) | NO   |     | NULL    |                |
| product_price       | float        | NO   |     | NULL    |                |
| product_image       | varchar(255) | NO   |     | NULL    |                |
+---------------------+--------------+------+-----+---------+----------------+

productsRDD.take(10).foreach(println)
productsRDD.count
res1: Long = 1345

// Join daily revenue per product id with products to get daily revenue per product (by name)
//final result format: (product_id, daily_revenue_per_product)
//for product, prepare a rdd like (product_id, product_name)
val productMap = productsRDD.
	map(product => (product.split(',')(0).toInt,product.split(',')(2)))
res2: (Int, String) = (1,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U)

//prepare orders rdd 
val orders = sc.textFile("/public/retail_db/orders")

//filter out orders not applicable
val ordersFiltered = orders.
  filter(order => order.split(",")(3) == "COMPLETE" || order.split(",")(3) == "CLOSED")
  
//create ordersMap with ()  
val ordersMap = ordersFiltered.
  map(order => (order.split(",")(0).toInt, order.split(",")(1)))

res4: (Int, String) = (1,2013-07-25 00:00:00.0)

//prepare orderItemsMap
val orderItems = sc.textFile("/public/retail_db/order_items")

val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt,(oi.split(",")(2).toInt, oi.split(",")(4).toFloat)))

res5: (Int, (Int, Float)) = (1,(957,299.98))
  
//Join orders and orderItems to get revenue info  
val ordersJoin = ordersMap.join(orderItemsMap)
val ordersJoinMap = ordersJoin.map(rec => ((rec._2._1, rec._2._2._1), rec._2._2._2))
res18: ((String, Int), Float) = ((2014-05-23,365),119.98)
val ordersJoinMap2 = ordersJoin.map(rec => (rec._2._1, (rec._2._2._1, rec._2._2._2)))
res19: (String, (Int, Float)) = (2014-05-23,(365,119.98))

val dailyRevenuePerProductId = ordersJoinMap.reduceByKey((revenue, order_item_subtotal) => revenue + order_item_subtotal)
res6: ((String, Int), Float) = ((2014-07-17 00:00:00.0,403),3379.7402)

//now change/transform the element location to make it like 
//((order_date, order_product_id), daily_revenue_per_product_id)
val dailyRevenuePerProductIdMap = dailyRevenuePerProductId.
  map(rec => (rec._1._2, (rec._1._1, rec._2)))
dailyRevenuePerProductIdMap.take(10).foreach(println)
//(order_product_id, (order_date, daily_revenue_per_product_id))
(403,(2014-07-17 00:00:00.0,3379.7402))
(982,(2013-11-21 00:00:00.0,149.99))
(116,(2013-10-11 00:00:00.0,224.95))
(564,(2014-01-06 00:00:00.0,60.0))
(885,(2013-11-06 00:00:00.0,74.97))
(572,(2014-02-26 00:00:00.0,199.95))
(1073,(2014-03-05 00:00:00.0,2399.8801))
(565,(2014-03-19 00:00:00.0,140.0))
(135,(2014-07-15 00:00:00.0,110.0))
(403,(2014-06-24 00:00:00.0,2079.84))

val dailyRevenuePerProductIdMap = dailyRevenuePerProductId.
  map(rec => (rec._1._2, (rec._1._1.substring(0,10), rec._2)))
(403,(2014-07-17,3379.7402))
(982,(2013-11-21,149.99))
(116,(2013-10-11,224.95))
(564,(2014-01-06,60.0))
(885,(2013-11-06,74.97))
(572,(2014-02-26,199.95))
(1073,(2014-03-05,2399.8801))
(565,(2014-03-19,140.0))
(135,(2014-07-15,110.0))
(403,(2014-06-24,2079.84))

val dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productMap)
//(order_product_id, ((order_date, daily_revenue_per_product_id), product_name))
(778,((2014-06-24,74.97),Bag Boy Beverage Holder))
(778,((2014-03-21,74.97),Bag Boy Beverage Holder))
(778,((2013-09-11,124.95),Bag Boy Beverage Holder))
(778,((2014-02-18,74.97),Bag Boy Beverage Holder))
(778,((2014-05-28,49.98),Bag Boy Beverage Holder))
(778,((2014-02-02,49.98),Bag Boy Beverage Holder))
(778,((2013-08-02,99.96),Bag Boy Beverage Holder))
(778,((2013-12-03,99.96),Bag Boy Beverage Holder))
(778,((2014-01-06,24.99),Bag Boy Beverage Holder))
(778,((2014-04-09,49.98),Bag Boy Beverage Holder))

//Sort the data by date in ascending order and by daily revenue per product in descending order
//first sort it by date 
val dailyRevenuePerProductJoinSorted1 = dailyRevenuePerProductIdMap.join(productMap).sortBy(_._2._1)
(897,((2013-07-25,49.98),Team Golf New England Patriots Putter Grip))
(835,((2013-07-25,63.98),Bridgestone e6 Straight Distance NFL Carolina))
(93,((2013-07-25,74.97),Under Armour Men's Tech II T-Shirt))
(926,((2013-07-25,79.95),Glove It Imperial Golf Towel))
(924,((2013-07-25,79.95),Glove It Urban Brick Golf Towel))
(810,((2013-07-25,79.96),Glove It Women's Mod Oval Golf Glove))
(828,((2013-07-25,95.97),Bridgestone e6 Straight Distance NFL San Dieg))
(906,((2013-07-25,99.96),Team Golf Tennessee Volunteers Putter Grip))
(134,((2013-07-25,100.0),Nike Women's Legend V-Neck T-Shirt))
(725,((2013-07-25,108.0),LIJA Women's Button Golf Dress))

//second, sort it by daily revenue per product in descending order
val dailyRevenuePerProductJoinSorted2 = dailyRevenuePerProductJoinSorted1.sortBy(_._2._1._2,false)
(793,((2014-05-23,14.99),Hirzl Women's Hybrid Golf Glove))
(793,((2014-06-14,14.99),Hirzl Women's Hybrid Golf Glove))
(793,((2014-06-24,14.99),Hirzl Women's Hybrid Golf Glove))
(792,((2014-06-24,14.99),Hirzl Men's Hybrid Golf Glove))
(792,((2014-06-28,14.99),Hirzl Men's Hybrid Golf Glove))
(792,((2014-06-30,14.99),Hirzl Men's Hybrid Golf Glove))
(793,((2014-07-02,14.99),Hirzl Women's Hybrid Golf Glove))
(793,((2014-07-24,14.99),Hirzl Women's Hybrid Golf Glove))
(775,((2013-08-27,9.99),Clicgear 8.0 Shoe Brush))
(775,((2013-08-28,9.99),Clicgear 8.0 Shoe Brush))
(775,((2013-09-08,9.99),Clicgear 8.0 Shoe Brush))
(775,((2013-10-08,9.99),Clicgear 8.0 Shoe Brush))
(775,((2013-10-13,9.99),Clicgear 8.0 Shoe Brush))
(775,((2013-10-30,9.99),Clicgear 8.0 Shoe Brush))

//get the data to desired format - order_date, dailyrevenue_per_product, product_name
val finalrdd = dailyRevenuePerProductJoinSorted2.map(rec => (rec._2._1._1, rec._2._1._2, rec._2._2))
finalrdd.take(10).foreach(println)
(2014-03-04,16399.182,Field & Stream Sportsman 16 Gun Fire Safe)
(2014-04-08,15999.201,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-12-06,15999.201,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-11-30,15599.223,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-11-03,15599.221,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-08-17,15199.242,Field & Stream Sportsman 16 Gun Fire Safe)
(2014-01-30,14799.262,Field & Stream Sportsman 16 Gun Fire Safe)
(2014-07-20,14799.262,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-12-11,14799.26,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-09-05,14399.282,Field & Stream Sportsman 16 Gun Fire Safe)

//save the rdd to text file format
scala> finalrdd.saveAsTextFile("/user/paslechoix/daily_revenue_per_product")

//verification
sc.textFile("/user/paslechoix/daily_revenue_per_product").take(10).foreach(println)
(2014-03-04,16399.182,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-12-06,15999.201,Field & Stream Sportsman 16 Gun Fire Safe)
(2014-04-08,15999.201,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-11-30,15599.223,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-11-03,15599.221,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-08-17,15199.242,Field & Stream Sportsman 16 Gun Fire Safe)
(2014-01-30,14799.262,Field & Stream Sportsman 16 Gun Fire Safe)
(2014-07-20,14799.262,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-12-11,14799.26,Field & Stream Sportsman 16 Gun Fire Safe)
(2013-09-05,14399.282,Field & Stream Sportsman 16 Gun Fire Safe)

// Copy both from HDFS to local file system
cd /home/paslechoix
mkdir daily_revenue_per_product
hadoop fs -get /user/paslechoix/daily_revenue_per_product \
// /home/paslechoix/daily_revenue_per_product/daily_revenue_txt_scala
// cd daily_revenue_per_product/daily_revenue_txt_scala/
// ls -ltr
[paslechoix@gw01 daily_revenue_per_product]$ ll
total 516
-rw-r--r-- 1 paslechoix students 246810 Jan 27 11:54 part-00000
-rw-r--r-- 1 paslechoix students 277476 Jan 27 11:54 part-00001
-rw-r--r-- 1 paslechoix students      0 Jan 27 11:54 _SUCCESS





