[paslechoix@gw03 ~]$ hdfs dfs -tail /public/retail_db/orders/part-00000
014-06-12 00:00:00.0,4229,PENDING
68861,2014-06-13 00:00:00.0,3031,PENDING_PAYMENT
68862,2014-06-15 00:00:00.0,7326,PROCESSING
68863,2014-06-16 00:00:00.0,3361,CLOSED
68864,2014-06-18 00:00:00.0,9634,ON_HOLD
68865,2014-06-19 00:00:00.0,4567,SUSPECTED_FRAUD
68866,2014-06-20 00:00:00.0,3890,PENDING_PAYMENT
68867,2014-06-23 00:00:00.0,869,CANCELED
68868,2014-06-24 00:00:00.0,10184,PENDING
68869,2014-06-25 00:00:00.0,7456,PROCESSING
68870,2014-06-26 00:00:00.0,3343,COMPLETE
68871,2014-06-28 00:00:00.0,4960,PENDING
68872,2014-06-29 00:00:00.0,3354,COMPLETE
68873,2014-06-30 00:00:00.0,4545,PENDING
68874,2014-07-03 00:00:00.0,1601,COMPLETE
68875,2014-07-04 00:00:00.0,10637,ON_HOLD
68876,2014-07-06 00:00:00.0,4124,COMPLETE
68877,2014-07-07 00:00:00.0,9692,ON_HOLD
68878,2014-07-08 00:00:00.0,6753,COMPLETE
68879,2014-07-09 00:00:00.0,778,COMPLETE
68880,2014-07-13 00:00:00.0,1117,COMPLETE
68881,2014-07-19 00:00:00.0,2518,PENDING_PAYMENT
68882,2014-07-22 00:00:00.0,10000,ON_HOLD
68883,2014-07-23 00:00:00.0,5533,COMPLETE


val orders = sc.textFile("/public/retail_db/orders")

scala> orders.count
res7: Long = 68883

scala> orders.first
res0: String = 1,2013-07-25 00:00:00.0,11599,CLOSED

Create <K, V>: <id, price>

scala> val ordersm = orders.map(x=>(x.split(",")(0).toInt, x.split(",")(2)))
res13: Array[(Int, String)] = Array((1,11599), (2,256), (3,12111), (4,8827), (5,11318), (6,7130), (7,4530), (8,2911), (9,5657), (10,5648), (11,918), (12,1837), (13,9149), (14,9842), (15,2568), (16,7276), (17,2667), (18,1205), (19,9488), (20,9198), (21,2711), (22,333), (23,4367), (24,11441), (25,9503), (26,7562), (27,3241), (28,656), (29,196), (30,10039), (31,6983), (32,3960), (33,5793), (34,4189), (35,4840), (36,5649), (37,5863), (38,11586), (39,8214), (40,12092), (41,8136), (42,9776), (43,7776), (44,10500), (45,2636), (46,1549), (47,8487), (48,12186), (49,1871), (50,5225), (51,12271), (52,5126), (53,4701), (54,10628), (55,2052), (56,10519), (57,7073), (58,9213), (59,11644), (60,8365), (61,4791), (62,9111), (63,1148), (64,5579), (65,5903), (66,1792), (67,1406), (68,4320), (69,2821), (70...

Count By Key
val cbk = ordersm.countByKey
cbk.collect
res36: Array[(Int, String)] = Array((1,11599), (2,256), (3,12111), (4,8827), (5,11318), (6,7130), (7,4530), (8,2911), (9,5657), (10,5648), (11,918), (12,1837), (13,9149), (14,9842), (15,2568), (16,7276), (17,2667), (18,1205), (19,9488), (20,9198), (21,2711), (22,333), (23,4367), (24,11441), (25,9503), (26,7562), (27,3241), (28,656), (29,196), (30,10039), (31,6983), (32,3960), (33,5793), (34,4189), (35,4840), (36,5649), (37,5863), (38,11586), (39,8214), (40,12092), (41,8136), (42,9776), (43,7776), (44,10500), (45,2636), (46,1549), (47,8487), (48,12186), (49,1871), (50,5225), (51,12271), (52,5126), (53,4701), (54,10628), (55,2052), (56,10519), (57,7073), (58,9213), (59,11644), (60,8365), (61,4791), (62,9111), (63,1148), (64,5579), (65,5903), (66,1792), (67,1406), (68,4320), (69,2821), (70...

Sort:

ascending:

scala> val asc = ordersm.sortBy(_._1)
asc.take(10).foreach(println)
(1,11599)
(2,256)
(3,12111)
(4,8827)
(5,11318)
(6,7130)
(7,4530)
(8,2911)
(9,5657)
(10,5648)

descending:

scala> val desc = ordersm.sortBy(_._1, false)
desc.take(10).foreach(println)
(68883,5533)
(68882,10000)
(68881,2518)
(68880,1117)
(68879,778)
(68878,6753)
(68877,9692)
(68876,4124)
(68875,10637)
(68874,1601)


Now, let's work on Status column as key 

val orders1 = orders.map(x=>(x.split(",")(3),x.split(",")(0)))

val orders1_w_count = orders1.countByKey

scala> orders1_w_count.take(10).foreach(println)
(PAYMENT_REVIEW,729)
(CLOSED,7556)
(SUSPECTED_FRAUD,1558)
(PROCESSING,8275)
(COMPLETE,22899)
(PENDING,7610)
(PENDING_PAYMENT,15030)
(ON_HOLD,3798)
(CANCELED,1428)

Now sorting
---------------------Stucked here-----------------------
on the count asc
scala> val asc1 = orders1_w_count.sortBy(_._1)
<console>:33: error: value sortBy is not a member of scala.collection.Map[String,Long]
         val asc1 = orders1_w_count.sortBy(_._1)


orders.map(order => (order.split(",")(3), "")).countByKey.foreach(println)


orders.map(order => (order.split(",")(3), "")).countByKey.sortBy(_.split(",")(1), false)
---------------------Stucked -----------------------



val orderItems = sc.textFile("/public/retail_db/order_items")
scala> orderItems.first
res2: String = 1,1,957,1,299.98,299.98

val orderItemsRevenue = orderItems.map(oi => oi.split(",")(4).toFloat)
orderItemsRevenue.first
res3: Float = 299.98

orderItemsRevenue.reduce((total, revenue) => total + revenue)
res4: Float = 3.4326256E7


val orderItemsMaxRevenue = orderItemsRevenue.reduce((max, revenue) => {
  if(max < revenue) revenue else max
})
orderItemsMaxRevenue: Float = 1999.99

val orderItemsMinRevenue = orderItemsRevenue.reduce((min, revenue) => {
  if(min > revenue) revenue else min
})
orderItemsMinRevenue: Float = 9.99


Aggregations - groupByKey examples

// Aggregations - groupByKey
//1, (1 to 1000) - sum(1 to 1000) => 1 + 2+ 3+ .....1000
//1, (1 to 1000) - sum(sum(1, 250), sum(251, 500), sum(501, 750), sum(751, 1000))
val orderItems = sc.textFile("/public/retail_db/order_items")

val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
scala> orderItemsMap.first
res5: (Int, Float) = (1,299.98)

val orderItemsGBK = orderItemsMap.groupByKey
orderItemsGBK.first 
res6: (Int, Iterable[Float]) = (41234,CompactBuffer(109.94))

//Get revenue per order_id
orderItemsGBK.map(rec => (rec._1, rec._2.toList.sum)).take(10).foreach(println)
(41234,109.94)
(65722,1319.8899)
(28730,349.95)
(68522,329.99)
(23776,329.98)
(32676,719.91003)
(53926,219.97)
(4926,939.85)
(38926,1049.9)
(29270,1379.8501)

//Get data in descending order by order_item_subtotal for each order_id
val ordersSortedByRevenue = orderItemsGBK.
  flatMap(rec => {
    rec._2.toList.sortBy(o => -o).map(k => (rec._1, k))
  })
ordersSortedByRevenue.take(10).foreach(println)
(41234,109.94)
(65722,400.0)
(65722,399.98)
(65722,199.98)
(65722,199.95)
(65722,119.98)
(28730,299.95)
(28730,50.0)
(68522,329.99)
(23776,199.99)



---------------pause here----------------------

// Aggregations - reduceByKey
val orderItems = sc.textFile("/public/retail_db/order_items")
val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
orderItemsMap.take(10).foreach(println)
(1,299.98)
(2,199.99)
(2,250.0)
(2,129.99)
(4,49.98)
(4,299.95)
(4,150.0)
(4,199.92)
(5,299.98)
(5,299.95)
  

val revenuePerOrderId = orderItemsMap.
  reduceByKey((total, revenue) => total + revenue)
scala> revenuePerOrderId.take(10).foreach(println)
(41234,109.94)
(65722,1319.8899)
(28730,349.95)
(68522,329.99)
(23776,329.98)
(32676,719.91003)
(53926,219.97)
(4926,939.85)
(38926,1049.9)
(29270,1379.8501)

order by first item (i.e. orderId _._1) in ascending order sortBy()
scala> revenuePerOrderId.sortBy(_._1).take(10).foreach(println)
(1,299.98)
(2,579.98)
(4,699.85004)
(5,1129.8601)
(7,579.92004)
(8,729.84)
(9,599.96)
(10,651.92)
(11,919.79004)
(12,1299.8701)

order by second item (i.e. revenue _._2) in ascending order sortBy()
scala> revenuePerOrderId.sortBy(_._2).take(10).foreach(println)
(41098,9.99)
(12380,9.99)
(21816,9.99)
(11102,9.99)
(23322,9.99)
(1944,9.99)
(5557,9.99)
(18772,14.99)
(7530,14.99)
(36546,14.99)


order by first item orderId
revenuePerOrderId.sortBy(_._0).take(10).foreach(println)

order by second item (i.e. revenue _._2) in descending order sortBy(, false)
scala> revenuePerOrderId.sortBy(_._2, false).take(10).foreach(println)
(68703,3449.91)
(68724,2859.89)
(68858,2839.91)
(68809,2779.8599)
(68766,2699.9)
(68806,2629.92)
(68821,2629.92)
(68778,2629.9)
(68848,2399.96)
(68875,2399.95)

Because for each OrderId there are multiple orderItems, to find out the min orderItems for each order:  

val minRevenuePerOrderId = orderItemsMap.reduceByKey((min, revenue) => if(min > revenue) revenue else min)
minRevenuePerOrderId.take(10).foreach(println)  
(41234,109.94)
(65722,119.98)
(28730,50.0)
(68522,329.99)
(23776,129.99)
(32676,59.99)
(53926,99.99)
(4926,199.92)
(38926,250.0)
(29270,119.98)

to find out the max orderItems for each order: 
val maxRevenuePerOrderId = orderItemsMap.reduceByKey((max, revenue) => if(max>revenue) max else revenue)

sort it by revenue in descending
scala> maxRevenuePerOrderId.sortBy(_._2, false).take(10).foreach(println)
(68703,3449.91)
(68724,2859.89)
(68858,2839.91)
(68809,2779.8599)
(68766,2699.9)
(68806,2629.92)
(68821,2629.92)
(68778,2629.9)
(68848,2399.96)
(68875,2399.95)


CountByKey

// Aggregations - using actions
val orders = sc.textFile("/public/retail_db/orders")
orders.map(order => (order.split(",")(3), "")).countByKey.foreach(println)
(PAYMENT_REVIEW,729)
(CLOSED,7556)
(SUSPECTED_FRAUD,1558)
(PROCESSING,8275)
(COMPLETE,22899)
(PENDING,7610)
(PENDING_PAYMENT,15030)
(ON_HOLD,3798)
(CANCELED,1428)

val orderItems = sc.textFile("/public/retail_db/order_items")
val orderItemsRevenue = orderItems.map(oi => oi.split(",")(4).toFloat)
orderItemsRevenues.take(10).foreach(println)
299.98
199.99
250.0
129.99
49.98
299.95
150.0
199.92
299.98
299.95


orderItemsRevenues.reduce((total, revenue) => total + revenue)
val orderItemsMaxRevenue = orderItemsRevenue.reduce((max, revenue) => {
  if(max < revenue) revenue else max
})
res14: Float = 3.4326256E7

val orderItemsMaxRevenue = orderItemsRevenue.reduce((max, revenue) => {
  if(max < revenue) revenue else max
})
orderItemsMaxRevenue: Float = 1999.99

SortByKey

// Ranking - Global (details of top 10 products)
val products = sc.textFile("/public/retail_db/products")

res0: String = 1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy

val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(4).toFloat, product))

res1: (Float, String) = (59.98,1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)
  
// Sorted By Key set to false, meaning it is sorted by value, in this case, key is Float the price and value is the product the String, as it is set to false, meaning sorted by Key, i.e. Float 
val productsSortedByPrice = productsMap.sortByKey(false)
productsSortedByPrice.take(10).foreach(println)
(1999.99,208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical)
(1799.99,66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1099.99,1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop)
(999.99,60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)
(999.99,695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)

val productsSortedByPrice2 = productsMap.sortBy(_._1, false).take(10).foreach(println)
(1999.99,208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical)
(1799.99,66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1099.99,1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop)
(999.99,60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)
(999.99,695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)


val products = sc.textFile("/public/retail_db/products")

products.
  filter(product => product.split(",")(4) != "").
  takeOrdered(10)(Ordering[Float].reverse.on(product => product.split(",")(4).toFloat)).
  foreach(println)
208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical
496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop
60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical
695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...
197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical
694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...
488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical

val productsMap2 = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product, product.split(",")(4).toFloat))
productsMap2.take(10).foreach(println)

productsMap2.sortBy(_._2, false).take(10).foreach(println)  
scala> productsMap2.sortBy(_._2, false).take(10).foreach(println)
(208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical,1999.99)
(66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill,1799.99)
(199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill,1799.99)
(496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill,1799.99)
(1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop,1099.99)
(60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical,999.99)
(197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical,999.99)
(488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical,999.99)
(694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...,999.99)
(695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...,999.99)

Note: as productsMap2 has been mapped to:
map(product => (product, product.split(",")(4).toFloat))
meaning the first element is product, the second element is price 
to sort by price, it should take _._2, there is no _._4



Top N			  
// Ranking - Get top N priced products with in each product category
val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))

scala> productsMap.take(10).foreach(println)
(2,1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)
(2,2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)
(2,3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
(2,4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
(2,5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet)
(2,6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat)
(2,7,2,Schutt Youth Recruit Hybrid Custom Football H,,99.99,http://images.acmesports.sports/Schutt+Youth+Recruit+Hybrid+Custom+Football+Helmet+2014)
(2,8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat)
(2,9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,,50.0,http://images.acmesports.sports/Nike+Adult+Vapor+Jet+3.0+Receiver+Gloves)
(2,10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)
  
Converting (K, V) pairs into (K, Iterable)  
val productsGroupByCategory = productsMap.groupByKey			  
productsGroupByCategory.first
res22: (Int, Iterable[String]) = (34,CompactBuffer(741,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 742,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 743,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 744,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 745,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 746,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 747,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 748,34,Ogio City Turf Golf Shoes,,129.99,htt...

The result shows tuple of category (which is an Int) and all the products in that category

Top N 

val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))
val productsGroupByCategory = productsMap.groupByKey
val productsIterable = productsGroupByCategory.first._2
get <K, V> of <productId, price>
val productPrices = productsIterable.map(p=> (p.split(",")(0), p.split(",")(4).toFloat))
productPrices.take(10).foreach(println)
(741,59.99)
(742,59.99)
(743,169.99)
(744,169.99)
(745,149.99)
(746,149.99)
(747,149.99)
(748,129.99)
(749,129.99)
(750,129.99)

//Get the top/bottom N items 
//initially there are 24 items
scala> productsIterable.map(p => p.split(",")(4).toFloat).size
res15: Int = 24

//convert it to Set will automatically group the same items 
scala> val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
productPrices: scala.collection.immutable.Set[Float] = Set(99.99, 169.99, 149.99, 59.99, 129.99, 34.99, 139.99)
scala> productPrices.size
res16: Int = 7

//Get the top n prices 
scala> val topNPrices = productPrices.toList.sortBy(p => -p).take(5)
topNPrices: List[Float] = List(169.99, 149.99, 139.99, 129.99, 99.99)

//Get the bottom n prices
scala> productPrices.toList.sorted
res18: List[Float] = List(34.99, 59.99, 99.99, 129.99, 139.99, 149.99, 169.99)

scala> productPrices.toList.sorted.take(5)
res19: List[Float] = List(34.99, 59.99, 99.99, 129.99, 139.99)

// Function to get top n priced products using Scala collections API

val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))

productsMap.take(10).foreach(println)  
(2,1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)
(2,2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)
(2,3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
(2,4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
(2,5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet)
(2,6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat)
(2,7,2,Schutt Youth Recruit Hybrid Custom Football H,,99.99,http://images.acmesports.sports/Schutt+Youth+Recruit+Hybrid+Custom+Football+Helmet+2014)
(2,8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat)
(2,9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,,50.0,http://images.acmesports.sports/Nike+Adult+Vapor+Jet+3.0+Receiver+Gloves)
(2,10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)


  
val productsGroupByCategory = productsMap.groupByKey
productsGroupByCategory.take(1).foreach(println)
(34,CompactBuffer(741,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 742,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 743,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 744,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 745,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 746,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 747,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 748,34,Ogio City Turf Golf Shoes,,129.99,http://images.acmesports.sports/Ogio+City+Turf+Golf+Shoes, 749,34,Ogio City Turf Golf Shoes,,129.99,http://images.acmesports.sports/Ogio+City+Turf+Golf+Shoes, 750,34,Ogio City Turf Golf Shoes,,129.99,http://images.acmesports.sports/Ogio+City+Turf+Golf+Shoes, 751,34,Ogio Sport Golf Shoes,,99.99,http://images.acmesports.sports/Ogio+Sport+Golf+Shoes, 752,34,Ogio Sport Golf Shoes,,99.99,http://images.acmesports.sports/Ogio+Sport+Golf+Shoes, 753,34,Ogio Sport Golf Shoes,,99.99,http://images.acmesports.sports/Ogio+Sport+Golf+Shoes, 754,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes, 755,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes, 756,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes, 757,34,TRUE linkswear Vegas Golf Shoes,,99.99,http://images.acmesports.sports/TRUE+linkswear+Vegas+Golf+Shoes, 758,34,TRUE linkswear Vegas Golf Shoes,,99.99,http://images.acmesports.sports/TRUE+linkswear+Vegas+Golf+Shoes, 759,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes, 760,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes, 761,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes, 762,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes, 763,34,PING Golf Shoe Bag,,34.99,http://images.acmesports.sports/PING+Golf+Shoe+Bag, 764,34,Nike Lunar Mount Royal Golf Shoes,,99.99,http://images.acmesports.sports/Nike+Lunar+Mount+Royal+Golf+Shoes))


def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {
  val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
  val topNPrices = productPrices.toList.sortBy(p => -p).take(topN)

  val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
  val minOfTopNPrices = topNPrices.min

  val topNPricedProducts = productsSorted.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)

  topNPricedProducts
}

val productsIterable = productsGroupByCategory.first._2
scala> productsIterable
res24: Iterable[String] = CompactBuffer(741,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 742,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 743,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 744,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 745,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 746,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 747,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 748,34,Ogio City Turf Golf Shoes,,129.99,http://images....


getTopNPricedProducts(productsIterable, 3).foreach(println)
743,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes
744,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes
745,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes
746,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes
747,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes
754,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes
755,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes
756,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes
759,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes
760,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes
761,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes
762,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes



