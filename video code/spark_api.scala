//Get details of top 5 customers by revenue for each month

| orders | CREATE TABLE `orders` (
  `order_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_date` datetime NOT NULL,
  `order_customer_id` int(11) NOT NULL,
  `order_status` varchar(45) NOT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB AUTO_INCREMENT=68884 DEFAULT CHARSET=utf8 |

val orderRDD = sc.textFile("/public/retail_db/orders")
res0: String = 1,2013-07-25 00:00:00.0,11599,CLOSED

val order_req = orderRDD.map(x=>(x.split(",")(0).toInt, x.split(",")(1).substring(0,10).replace("-","").toInt, x.split(",")(2).toFloat, x.split(",")(3)))
res9: (Int, Int, Float, String) = (1,20130725,11599.0,CLOSED)

val order_reqDF = order_req.toDF("order_id","order_date","order_customer_id","order_status")
order_reqDF.registerTempTable("orders")

//To handle the case that some orders do not have price, note "" not ''
val order_req1 = orderRDD.map(x=>(x.split(",")(0).toInt, x.split(",")(1).substring(0,10).replace("-","").toInt, {if(x.split(",")(2)==null || x.split(",")(2) =="") 0 else x.split(",")(2).toFloat}, x.split(",")(3)))

| order_items | CREATE TABLE `order_items` (
  `order_item_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_item_order_id` int(11) NOT NULL,
  `order_item_product_id` int(11) NOT NULL,
  `order_item_quantity` tinyint(4) NOT NULL,
  `order_item_subtotal` float NOT NULL,
  `order_item_product_price` float NOT NULL,
  PRIMARY KEY (`order_item_id`)
) ENGINE=InnoDB AUTO_INCREMENT=172199 DEFAULT CHARSET=utf8 |

val order_itemRDD = sc.textFile("/public/retail_db/order_items")
res11: String = 1,1,957,1,299.98,299.98

val order_item_req = order_itemRDD.map(a => (a.split(",")(0).toInt, a.split(",")(1).toInt,a.split(",")(2).toInt,a.split(",")(3).toInt,a.split(",")(4).toFloat,a.split(",")(5).toFloat))
val order_item_reqDF = order_item_req.toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity", "order_item_subtotal", "order_item_product_price")
order_item_reqDF.registerTempTable("order_items")


| customers | CREATE TABLE `customers` (
  `customer_id` int(11) NOT NULL AUTO_INCREMENT,
  `customer_fname` varchar(45) NOT NULL,
  `customer_lname` varchar(45) NOT NULL,
  `customer_email` varchar(45) NOT NULL,
  `customer_password` varchar(45) NOT NULL,
  `customer_street` varchar(255) NOT NULL,
  `customer_city` varchar(45) NOT NULL,
  `customer_state` varchar(45) NOT NULL,
  `customer_zipcode` varchar(45) NOT NULL,
  PRIMARY KEY (`customer_id`)
) ENGINE=InnoDB AUTO_INCREMENT=12436 DEFAULT CHARSET=utf8 |

val customerRDD = sc.textFile("/public/retail_db/customers")
res12: String = 1,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville,TX,78521

val customer_req = customerRDD.map(a => (a.split(",")(0).toInt,a))
res13: (Int, String) = (1,1,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville,TX,78521)

val customer_reqDF = customer_req.toDF("customer_id","customer_detail")
customer_reqDF.registerTempTable("customers")


//Now we have three tables: orders, order_items, customers
//top 5 customers by revenue for each month

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!DOESN'T MATCH!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

select substring(replace(substring(order_date, 1,10), '-',''),1, 6) as OrderMonth, c.customer_id, round(sum(oi.order_item_subtotal),2) as MonthlyTotal
from orders o join order_items oi on oi.order_item_order_id = o.order_id
join customers c on c.customer_id = o.order_customer_id
group by substring(replace(substring(order_date, 1,10), '-',''),1, 6), customer_id 
order by sum(oi.order_item_subtotal) desc 
limit 5;
+------------+-------------+--------------+
| OrderMonth | customer_id | MonthlyTotal |
+------------+-------------+--------------+
| 201403     |       10351 |      4489.65 |
| 201308     |        9515 |      4229.84 |
| 201401     |           7 |      4139.68 |
| 201406     |       12284 |      4139.33 |
| 201404     |        2564 |      4069.84 |
+------------+-------------+--------------+


verification:

select substring(replace(substring(order_date, 1,10), '-',''),1, 6) as OrderMonth, c.customer_id, oi.order_item_subtotal
from orders o join order_items oi on oi.order_item_order_id = o.order_id
join customers c on c.customer_id = o.order_customer_id
where cast(order_date as char) like '%2014-01-%' and c.customer_id = 7
group by substring(replace(substring(order_date, 1,10), '-',''),1, 6), customer_id, oi.order_item_subtotal;
+------------+-------------+---------------------+
| OrderMonth | customer_id | order_item_subtotal |
+------------+-------------+---------------------+
| 201401     |           7 |                 100 |
| 201401     |           7 |              129.99 |
| 201401     |           7 |                 150 |
| 201401     |           7 |              199.92 |
| 201401     |           7 |              199.98 |
| 201401     |           7 |              299.98 |
| 201401     |           7 |              399.98 |
| 201401     |           7 |              499.95 |
+------------+-------------+---------------------+


select substring(replace(substring(order_date, 1,10), '-',''),1, 6) as OrderMonth, c.customer_id, oi.order_item_subtotal
from orders o join order_items oi on oi.order_item_order_id = o.order_id
join customers c on c.customer_id = o.order_customer_id
where cast(order_date as char) like '%2014-01-%' and c.customer_id = 7
group by order_date, customer_id, oi.order_item_subtotal;
+------------+-------------+---------------------+
| OrderMonth | customer_id | order_item_subtotal |
+------------+-------------+---------------------+
| 201401     |           7 |              129.99 |
| 201401     |           7 |              199.92 |
| 201401     |           7 |              399.98 |
| 201401     |           7 |                 150 |
| 201401     |           7 |              199.98 |
| 201401     |           7 |              399.98 |
| 201401     |           7 |              499.95 |
| 201401     |           7 |                 100 |
| 201401     |           7 |              129.99 |
| 201401     |           7 |              299.98 |
| 201401     |           7 |              399.98 |
| 201401     |           7 |              129.99 |
| 201401     |           7 |              399.98 |
+------------+-------------+---------------------+



!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!DOESN'T MATCH!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

//Continue with the first query which generates the right result, the result will be saved locally

val sql_query = """select substring(replace(substring(order_date, 1,10), '-',''),1, 6) as OrderMonth, c.customer_id, round(sum(oi.order_item_subtotal),2) as MonthlyTotal
from orders o join order_items oi on oi.order_item_order_id = o.order_id
join customers c on c.customer_id = o.order_customer_id
group by substring(replace(substring(order_date, 1,10), '-',''),1, 6), customer_id 
order by sum(oi.order_item_subtotal) desc 
limit 5"""
val final_result = sqlContext.sql(sql_query)

18/03/14 14:03:58 INFO ParseDriver: Parse Completed
org.apache.spark.sql.AnalysisException: undefined function replace; line 4 pos 60


val sql_query2 = """select substring(regexp_replace(substring(order_date, 1,10), '-',''),1, 6) as OrderMonth, c.customer_id, round(sum(oi.order_item_subtotal),2) as MonthlyTotal
from orders o join order_items oi on oi.order_item_order_id = o.order_id
join customers c on c.customer_id = o.order_customer_id
group by substring(regexp_replace(substring(order_date, 1,10), '-',''),1, 6), customer_id 
order by sum(oi.order_item_subtotal) desc 
limit 5"""

val final_result = sqlContext.sql(sql_query2)
final_result.show
+----------+-----------+------------+
|OrderMonth|customer_id|MonthlyTotal|
+----------+-----------+------------+
|    201403|      10351|     4489.65|
|    201308|       9515|     4229.84|
|    201401|          7|     4139.68|
|    201406|      12284|     4139.33|
|    201404|       2564|     4069.84|
+----------+-----------+------------+


final_result.rdd.saveAsTextFile("final_result")
[paslechoix@gw03 ~]$ hdfs dfs -cat final_result/*
[201403,10351,4489.65]
[201308,9515,4229.84]
[201401,7,4139.68]
[201406,12284,4139.33]
[201404,2564,4069.84]
[paslechoix@gw03 ~]$






mysql> select substring(replace(substring("2014-03-02 00:00:00", 1,10), '-',''),1, 6);
mysql> select substring("2014-03-02 00:00:00", 0,10);



val order_order_item_join = order_req.join(order_item_req)

val sum_total = order_order_item_join.map(e => e.2).reduceByKey(+_).map(e => (e._1._1,(e._1._2,e._2)))

val date_group = sum_total.groupByKey()

def getTopNcustomer(a: (Int, Iterable[(Int, Float)]), topn: Int) = {
val x = a._2.toList.sortBy(o => -o._2)
val high_total = x.map(t => t._2).distinct.take(topn).min
val final_customer = x.takeWhile(q => q._2.toFloat >=high_total)
final_customer
}

val filter_cus = date_group.flatMap(e => getTopNcustomer(e, 5).map(a => (e._1,a)))

val rearrange = filter_cus.map(c => (c._2._1,(c._2._2,c._1)))

val customer_join = rearrange.join(customer_req).map(c => c._2).sortBy(o => (o._1._2,-o._1._1))

val final_cust = customer_join.map(c => c._2 + “,” + c._1._2 +","+c._1._1)