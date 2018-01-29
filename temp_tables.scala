//create RDDs
//RDD: orders
val ordersRDD = sc.textFile("/public/retail_db/orders")
//convert to DF
val ordersDF = ordersRDD.map(order => {
  (order.split(",")(0).toInt, order.split(",")(1).dropRight(11).replace("-",""), order.split(",")(2).toInt, order.split(",")(3))
  }).toDF("order_id", "order_date", "order_customer_id", "order_status")
//register temp table
ordersDF.registerTempTable("orders")

//RDD: order_items
val order_itemsRDD = sc.textFile("/public/retail_db/order_items")
//convert to DF
val order_itemsDF = order_itemsRDD.map(oi=>{(oi.split(",")(0).toInt, oi.split(",")(1).toInt, oi.split(",")(2).toInt, oi.split(",")(3).toInt, oi.split(",")(4).toFloat, oi.split(",")(5).toFloat)}).
toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price")
//register temp table
order_itemsDF.registerTempTable("order_items")

//RDD: products
val productsRDD = sc.textFile("/public/retail_db/products")
//convert to DF
//val productsDF = productsRDD.map(p=>{(p.split(",")(0).toInt, p.split(",")(1).toInt, p.split(",")(2), p.split(",")(3).toInt, p.split(",")(4), p.split(",")(5).toFloat)}).toDF("product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image")

//we don't need all the fields from product
val productsDF = productsRDD.map(p=>{(p.split(",")(0).toInt, p.split(",")(2))}).toDF("product_id",  "product_name")

//register temp table
productsDF.registerTempTable("products")

sqlContext.sql("
SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product " +
"FROM orders o JOIN order_items oi " +
"ON o.order_id = oi.order_item_order_id " +
"JOIN products p ON p.product_id = oi.order_item_product_id " +
"WHERE o.order_status IN ('COMPLETE', 'CLOSED') " +
"GROUP BY o.order_date, p.product_name " +
"ORDER BY o.order_date, daily_revenue_per_product desc").
show

sqlContext.sql(
"SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product from orders o join order_items oi on o.order_id= o.order_id join products p on p.product_id = oi.order_item_product_id where o.order_status in ('complete', 'closed') group by o.order_date, p.product_name order by o.order_date, daily_revenue_per_product desc").
show





--mysql:
mysql> SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product
    -> from orders o join order_items oi on o.order_id = o.order_id
    -> join products p on p.product_id = oi.order_item_product_id
    -> where o.order_status in ('complete', 'closed')
    -> group by o.order_date, p.product_name
    -> order by o.order_date, daily_revenue_per_product desc;
SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product from orders o join order_items oi on o.order_id= o.order_id join products p on p.product_id = oi.order_item_product_id where o.order_status in ('complete', 'closed') group by o.order_date, p.product_name order by o.order_date, daily_revenue_per_product desc;
