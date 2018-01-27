val orders = sc.textFile("/public/retail_db/orders")
val orderItems = sc.textFile("/public/retail_db/order_items")
val ordersFiltered = orders.filter(order => (order.split(",")(3) == "CLOSED" || order.split(",")(3) == "COMPLETE"))

val ordersFiltered = orders.
  filter(order => order.split(",")(3) == "COMPLETE" || order.split(",")(3) == "CLOSED")

// Convert filtered orders to key value pair <orderId, orderDate>
val ordersMap = ordersFiltered.
  map(order => (order.split(",")(0).toInt, order.split(",")(1)))
val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt,(oi.split(",")(2).toInt, oi.split(",")(4).toFloat)))
val ordersJoin = ordersMap.join(orderItemsMap)
val ordersJoinMap = ordersJoin.map(rec => ((rec._2._1, rec._2._2._1), rec._2._2._2))
val dailyRevenuePerProductId = ordersJoinMap.
  reduceByKey((revenue, order_item_subtotal) => revenue + order_item_subtotal)
dailyRevenuePerProductId.take(10).sortBy(_._2, false).foreach(println)  