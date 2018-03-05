// Aggregations - reduceByKey

val order_items = sc.textFile("/public/retail_db/order_items")
scala> val order_items_map = order_items.map(x=>(x.split(",")(1).toInt, x.split(",")(4).toFloat))
res0: (Int, Float) = (1,299.98)

val revenuePerOrderId = order_items_map.reduceByKey((a, b) => a + b)
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


scala> revenuePerOrderId.sortByKey().take(10).foreach(println)
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


order by first item (i.e. orderId _._2) in descending order sortBy()
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

Compare to mysql:
mysql> select order_item_order_id, round(sum(order_item_subtotal),2) as subtotal from order_items group by order_item_order_id order by subtotal desc limit 10;
+---------------------+----------+
| order_item_order_id | subtotal |
+---------------------+----------+
|               68703 |  3449.91 |
|               68724 |  2859.89 |
|               68858 |  2839.91 |
|               68809 |  2779.86 |
|               68766 |  2699.90 |
|               68821 |  2629.92 |
|               68806 |  2629.92 |
|               68778 |  2629.90 |
|               68848 |  2399.96 |
|               68875 |  2399.95 |
+---------------------+----------+
10 rows in set (0.11 sec)


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


Because for each OrderId there are multiple orderItems, to find out the min orderItems for each order:  

val minRevenuePerOrderId = order_items_map.reduceByKey((min, revenue) => if(min > revenue) revenue else min).sortBy(_._2)
minRevenuePerOrderId.take(56).sortBy(_._1).foreach(println)  
(234,9.99)
(1944,9.99)
(2023,9.99)
(2277,9.99)
(5404,9.99)
(5557,9.99)
(5597,9.99)
(6177,9.99)
(7361,9.99)
(7387,9.99)
(10554,9.99)
(11102,9.99)
(12380,9.99)
(12944,9.99)
(12988,9.99)
(15290,9.99)
(15659,9.99)
(16805,9.99)
(17969,9.99)
(18342,9.99)
(21370,9.99)
(21816,9.99)
(22009,9.99)
(22880,9.99)
(23322,9.99)
(25478,9.99)
(26526,9.99)
(27201,9.99)
(28198,9.99)
(29116,9.99)
(29493,9.99)
(30356,9.99)
(31937,9.99)
(32319,9.99)
(32693,9.99)
(33627,9.99)
(33648,9.99)
(34233,9.99)
(37332,9.99)
(40394,9.99)
(41098,9.99)
(42108,9.99)
(42893,9.99)
(43175,9.99)
(43589,9.99)
(43925,9.99)
(44791,9.99)
(47334,9.99)
(48544,9.99)
(49037,9.99)
(50501,9.99)
(50644,9.99)
(53162,9.99)
(54100,9.99)
(54682,9.99)
(57042,9.99)
;

Compare to mysql:
mysql> select order_item_order_id, min(order_item_subtotal) from order_items group by order_item_order_id order by min(order_item_subtotal), order_item_order_id limit 56;
+---------------------+--------------------------+
| order_item_order_id | min(order_item_subtotal) |
+---------------------+--------------------------+
|                 234 |                     9.99 |
|                1944 |                     9.99 |
|                2023 |                     9.99 |
|                2277 |                     9.99 |
|                5404 |                     9.99 |
|                5557 |                     9.99 |
|                5597 |                     9.99 |
|                6177 |                     9.99 |
|                7361 |                     9.99 |
|                7387 |                     9.99 |
|               10554 |                     9.99 |
|               11102 |                     9.99 |
|               12380 |                     9.99 |
|               12944 |                     9.99 |
|               12988 |                     9.99 |
|               15290 |                     9.99 |
|               15659 |                     9.99 |
|               16805 |                     9.99 |
|               17969 |                     9.99 |
|               18342 |                     9.99 |
|               21370 |                     9.99 |
|               21816 |                     9.99 |
|               22009 |                     9.99 |
|               22880 |                     9.99 |
|               23322 |                     9.99 |
|               25478 |                     9.99 |
|               26526 |                     9.99 |
|               27201 |                     9.99 |
|               28198 |                     9.99 |
|               29116 |                     9.99 |
|               29493 |                     9.99 |
|               30356 |                     9.99 |
|               31937 |                     9.99 |
|               32319 |                     9.99 |
|               32693 |                     9.99 |
|               33627 |                     9.99 |
|               33648 |                     9.99 |
|               34233 |                     9.99 |
|               37332 |                     9.99 |
|               40394 |                     9.99 |
|               41098 |                     9.99 |
|               42108 |                     9.99 |
|               42893 |                     9.99 |
|               43175 |                     9.99 |
|               43589 |                     9.99 |
|               43925 |                     9.99 |
|               44791 |                     9.99 |
|               47334 |                     9.99 |
|               48544 |                     9.99 |
|               49037 |                     9.99 |
|               50501 |                     9.99 |
|               50644 |                     9.99 |
|               53162 |                     9.99 |
|               54100 |                     9.99 |
|               54682 |                     9.99 |
|               57042 |                     9.99 |
+---------------------+--------------------------+
56 rows in set (0.13 sec)



to find out the max orderItems for each order: 
val maxRevenuePerOrderId = order_items_map.reduceByKey((a,b) => if(a>b) a else b)

sort it by revenue in descending

scala> maxRevenuePerOrderId.sortBy(_._2,false).take(15).sortBy(_._1).foreach(println)
(68703,1999.99)
(68722,1999.99)
(68724,1999.99)
(68736,1999.99)
(68766,1999.99)
(68778,1999.99)
(68806,1999.99)
(68809,1999.99)
(68821,1999.99)
(68837,1999.99)
(68848,1999.99)
(68858,1999.99)
(68859,1999.99)
(68875,1999.99)
(68883,1999.99)

Compare to mysql:
mysql> select order_item_order_id, max(order_item_subtotal) from order_items group by order_item_order_id order by max(order_item_subtotal) desc, order_item_order_id limit 15;
+---------------------+--------------------------+
| order_item_order_id | max(order_item_subtotal) |
+---------------------+--------------------------+
|               68703 |                  1999.99 |
|               68722 |                  1999.99 |
|               68724 |                  1999.99 |
|               68736 |                  1999.99 |
|               68766 |                  1999.99 |
|               68778 |                  1999.99 |
|               68806 |                  1999.99 |
|               68809 |                  1999.99 |
|               68821 |                  1999.99 |
|               68837 |                  1999.99 |
|               68848 |                  1999.99 |
|               68858 |                  1999.99 |
|               68859 |                  1999.99 |
|               68875 |                  1999.99 |
|               68883 |                  1999.99 |
+---------------------+--------------------------+
15 rows in set (0.14 sec)

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


