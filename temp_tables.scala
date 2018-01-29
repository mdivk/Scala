//create RDDs
//RDD: orders
val ordersRDD = sc.textFile("/public/retail_db/orders")
ordersRDD.take(5).foreach(println)
1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
3,2013-07-25 00:00:00.0,12111,COMPLETE
4,2013-07-25 00:00:00.0,8827,CLOSED
5,2013-07-25 00:00:00.0,11318,COMPLETE

//convert to DF
val ordersDF = ordersRDD.map(order => {
  (order.split(",")(0).toInt, order.split(",")(1).dropRight(11).replace("-",""), order.split(",")(2).toInt, order.split(",")(3))
  }).toDF("order_id", "order_date", "order_customer_id", "order_status")
//register temp table
ordersDF.registerTempTable("orders")

//RDD: order_items
val order_itemsRDD = sc.textFile("/public/retail_db/order_items")
order_itemsRDD.take(5).foreach(println)
1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
5,4,897,2,49.98,24.99


//convert to DF
val order_itemsDF = order_itemsRDD.map(oi=>{(oi.split(",")(0).toInt, oi.split(",")(1).toInt, oi.split(",")(2).toInt, oi.split(",")(3).toInt, oi.split(",")(4).toFloat, oi.split(",")(5).toFloat)}).
toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price")
//register temp table
order_itemsDF.registerTempTable("order_items")

//RDD: products
val productsRDD = sc.textFile("/public/retail_db/products")
productsRDD.take(5).foreach(println)
1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy
2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat
4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat
5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet

//convert to DF
//val productsDF = productsRDD.map(p=>{(p.split(",")(0).toInt, p.split(",")(1).toInt, p.split(",")(2), p.split(",")(3).toInt, p.split(",")(4), p.split(",")(5).toFloat)}).toDF("product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image")

//we don't need all the fields from product
val productsDF = productsRDD.map(p=>{(p.split(",")(0).toInt, p.split(",")(2))}).toDF("product_id",  "product_name")

//register temp table
productsDF.registerTempTable("products")

sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product FROM orders o JOIN order_items oi ON o.order_id = oi.order_item_order_id JOIN products p ON p.product_id = oi.order_item_product_id WHERE o.order_status IN ('COMPLETE', 'CLOSED') GROUP BY o.order_date, p.product_name ORDER BY o.order_date, daily_revenue_per_product desc").show

+----------+--------------------+-------------------------+
|order_date|        product_name|daily_revenue_per_product|
+----------+--------------------+-------------------------+
|  20130725|Field & Stream Sp...|        5599.720153808594|
|  20130725|Nike Men's Free 5...|        5099.490051269531|
|  20130725|Diamondback Women...|        4499.700164794922|
|  20130725|Perfect Fitness P...|       3359.4401054382324|
|  20130725|Pelican Sunstream...|        2999.850082397461|
|  20130725|O'Brien Men's Neo...|       2798.8799781799316|
|  20130725|Nike Men's CJ Eli...|        1949.850082397461|
|  20130725|Nike Men's Dri-FI...|                   1650.0|
|  20130725|Under Armour Girl...|       1079.7300071716309|
|  20130725|Bowflex SelectTec...|         599.989990234375|
|  20130725|Elevation Trainin...|        319.9599914550781|
|  20130725|Titleist Pro V1 H...|        207.9600067138672|
|  20130725|Nike Men's Kobe I...|       199.99000549316406|
|  20130725|Cleveland Golf Wo...|       119.98999786376953|
|  20130725|TYR Boys' Team Di...|       119.97000122070312|
|  20130725|Merrell Men's All...|       109.98999786376953|
|  20130725|LIJA Women's Butt...|                    108.0|
|  20130725|Nike Women's Lege...|                    100.0|
|  20130725|Team Golf Tenness...|        99.95999908447266|
|  20130725|Bridgestone e6 St...|        95.97000122070312|
+----------+--------------------+-------------------------+


--mysql:
mysql> SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product from orders o join order_items oi on o.order_id= o.order_id join products p on p.product_id = oi.order_item_product_id where o.order_status in ('complete', 'closed') group by o.order_date, p.product_name order by o.order_date, daily_revenue_per_product desc limit 20;

+---------------------+-----------------------------------------------+---------------------------+
| order_date          | product_name                                  | daily_revenue_per_product |
+---------------------+-----------------------------------------------+---------------------------+
| 2013-07-25 00:00:00 | Field & Stream Sportsman 16 Gun Fire Safe     |         5599.720153808594 |
| 2013-07-25 00:00:00 | Nike Men's Free 5.0+ Running Shoe             |         5099.490051269531 |
| 2013-07-25 00:00:00 | Diamondback Women's Serene Classic Comfort Bi |         4499.700164794922 |
| 2013-07-25 00:00:00 | Perfect Fitness Perfect Rip Deck              |        3359.4401054382324 |
| 2013-07-25 00:00:00 | Pelican Sunstream 100 Kayak                   |         2999.850082397461 |
| 2013-07-25 00:00:00 | O'Brien Men's Neoprene Life Vest              |        2798.8799781799316 |
| 2013-07-25 00:00:00 | Nike Men's CJ Elite 2 TD Football Cleat       |         1949.850082397461 |
| 2013-07-25 00:00:00 | Nike Men's Dri-FIT Victory Golf Polo          |                      1650 |
| 2013-07-25 00:00:00 | Under Armour Girls' Toddler Spine Surge Runni |        1079.7300071716309 |
| 2013-07-25 00:00:00 | Bowflex SelectTech 1090 Dumbbells             |          599.989990234375 |
| 2013-07-25 00:00:00 | Elevation Training Mask 2.0                   |         319.9599914550781 |
| 2013-07-25 00:00:00 | Titleist Pro V1 High Numbers Personalized Gol |         207.9600067138672 |
| 2013-07-25 00:00:00 | Nike Men's Kobe IX Elite Low Basketball Shoe  |        199.99000549316406 |
| 2013-07-25 00:00:00 | Cleveland Golf Women's 588 RTX CB Satin Chrom |        119.98999786376953 |
| 2013-07-25 00:00:00 | TYR Boys' Team Digi Jammer                    |        119.97000122070312 |
| 2013-07-25 00:00:00 | Merrell Men's All Out Flash Trail Running Sho |        109.98999786376953 |
| 2013-07-25 00:00:00 | LIJA Women's Button Golf Dress                |                       108 |
| 2013-07-25 00:00:00 | Nike Women's Legend V-Neck T-Shirt            |                       100 |
| 2013-07-25 00:00:00 | Team Golf Tennessee Volunteers Putter Grip    |         99.95999908447266 |
| 2013-07-25 00:00:00 | Bridgestone e6 Straight Distance NFL San Dieg |         95.97000122070312 |
+---------------------+-----------------------------------------------+---------------------------+


sqlContext.sql("CREATE DATABASE paslechoix")
sqlContext.sql("CREATE TABLE paslechoix.daily_revenue (order_date string, product_name string, daily_revenue_per_product float) STORED AS orc")

val daily_revenue_per_productDF = sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product FROM orders o JOIN order_items oi ON o.order_id = oi.order_item_order_id JOIN products p ON p.product_id = oi.order_item_product_id WHERE o.order_status IN ('COMPLETE', 'CLOSED') GROUP BY o.order_date, p.product_name ORDER BY o.order_date, daily_revenue_per_productDF desc")

daily_revenue_per_productDF.show(5)
+----------+--------------------+-------------------------+
|order_date|        product_name|daily_revenue_per_product|
+----------+--------------------+-------------------------+
|  20130725|Field & Stream Sp...|        5599.720153808594|
|  20130725|Nike Men's Free 5...|        5099.490051269531|
|  20130725|Diamondback Women...|        4499.700164794922|
|  20130725|Perfect Fitness P...|       3359.4401054382324|
|  20130725|Pelican Sunstream...|        2999.850082397461|
+----------+--------------------+-------------------------+


daily_revenue_per_productDF.insertInto("paslechoix.daily_revenue")
sqlContext.sql("Select count(1) from paslechoix.daily_revenue")
|  _c0|
+-----+
|18240|

sqlContext.sql("Select * from paslechoix.daily_revenue limit 5").show(truncate=false)

+----------+---------------------------------------------+-------------------------+
|order_date|product_name                                 |daily_revenue_per_product|
+----------+---------------------------------------------+-------------------------+
|20130725  |Field & Stream Sportsman 16 Gun Fire Safe    |5599.72                  |
|20130725  |Nike Men's Free 5.0+ Running Shoe            |5099.49                  |
|20130725  |Diamondback Women's Serene Classic Comfort Bi|4499.7                   |
|20130725  |Perfect Fitness Perfect Rip Deck             |3359.4402                |
|20130725  |Pelican Sunstream 100 Kayak                  |2999.85                  |
+----------+---------------------------------------------+-------------------------+

//Verify in hive 


hive (paslechoix)> desc daily_revenue;
OK
order_date              string
product_name            string
daily_revenue_per_product       float

hive (paslechoix)> Select * from paslechoix.daily_revenue limit 5;
OK
20130725        Field & Stream Sportsman 16 Gun Fire Safe       5599.72
20130725        Nike Men's Free 5.0+ Running Shoe       		5099.49
20130725        Diamondback Women's Serene Classic Comfort Bi   4499.7
20130725        Perfect Fitness Perfect Rip Deck        		3359.4402
20130725        Pelican Sunstream 100 Kayak     				2999.85

//save the result to json format onto hdfs 
scala> daily_revenue_per_productDF.save("/user/paslechoix/daily_revenue_save", "json")
[paslechoix@gw01 ~]$ hdfs dfs -cat /user/paslechoix/daily_revenue_save/part-r-00191-38f3e6f3-c487-4536-aee3-0329f0572a52
{"order_date":"20140708","product_name":"Team Golf Texas Longhorns Putter Grip","daily_revenue_per_product":24.989999771118164}
{"order_date":"20140708","product_name":"Top Flite Women's 2014 XL Hybrid","daily_revenue_per_product":19.989999771118164}
{"order_date":"20140709","product_name":"Field & Stream Sportsman 16 Gun Fire Safe","daily_revenue_per_product":5599.720153808594}
{"order_date":"20140709","product_name":"Perfect Fitness Perfect Rip Deck","daily_revenue_per_product":5099.150131225586}
{"order_date":"20140709","product_name":"Diamondback Women's Serene Classic Comfort Bi","daily_revenue_per_product":4499.700164794922}
{"order_date":"20140709","product_name":"Nike Men's Free 5.0+ Running Shoe","daily_revenue_per_product":4499.550033569336}
{"order_date":"20140709","product_name":"Pelican Sunstream 100 Kayak","daily_revenue_per_product":3799.810104370117}
{"order_date":"20140709","product_name":"Nike Men's CJ Elite 2 TD Football Cleat","daily_revenue_per_product":3509.7301483154297}
{"order_date":"20140709","product_name":"Nike Men's Dri-FIT Victory Golf Polo","daily_revenue_per_product":3050.0}
{"order_date":"20140709","product_name":"O'Brien Men's Neoprene Life Vest","daily_revenue_per_product":2498.9999809265137}

//another way to write to hdfs in json format 
scala> daily_revenue_per_productDF.write.json("/user/paslechoix/daily_revenue_write")
[paslechoix@gw01 ~]$ hdfs dfs -cat /user/paslechoix/daily_revenue_write/part-r-00199-8eb99f57-8357-4413-b8a5-9716e0712588
{"order_date":"20140723","product_name":"SOLE E35 Elliptical","daily_revenue_per_product":1999.989990234375}
{"order_date":"20140723","product_name":"O'Brien Men's Neoprene Life Vest","daily_revenue_per_product":1999.1999816894531}
{"order_date":"20140723","product_name":"Under Armour Girls' Toddler Spine Surge Runni","daily_revenue_per_product":1959.510009765625}
{"order_date":"20140723","product_name":"LIJA Women's Eyelet Sleeveless Golf Polo","daily_revenue_per_product":325.0}
{"order_date":"20140723","product_name":"Titleist Small Wheeled Travel Cover","daily_revenue_per_product":249.99000549316406}
{"order_date":"20140723","product_name":"adidas Youth Germany Black/Red Away Match Soc","daily_revenue_per_product":210.0}
{"order_date":"20140723","product_name":"Nike Men's Deutschland Weltmeister Winners Bl","daily_revenue_per_product":120.0}

//select directly from dataframe
scala> daily_revenue_per_productDF.select("order_date", "product_name", "daily_revenue_per_product").show(10,truncate=false)
+----------+---------------------------------------------+-------------------------+
|order_date|product_name                                 |daily_revenue_per_product|
+----------+---------------------------------------------+-------------------------+
|20130725  |Field & Stream Sportsman 16 Gun Fire Safe    |5599.720153808594        |
|20130725  |Nike Men's Free 5.0+ Running Shoe            |5099.490051269531        |
|20130725  |Diamondback Women's Serene Classic Comfort Bi|4499.700164794922        |
|20130725  |Perfect Fitness Perfect Rip Deck             |3359.4401054382324       |
|20130725  |Pelican Sunstream 100 Kayak                  |2999.850082397461        |
|20130725  |O'Brien Men's Neoprene Life Vest             |2798.8799781799316       |
|20130725  |Nike Men's CJ Elite 2 TD Football Cleat      |1949.850082397461        |
|20130725  |Nike Men's Dri-FIT Victory Golf Polo         |1650.0                   |
|20130725  |Under Armour Girls' Toddler Spine Surge Runni|1079.7300071716309       |
|20130725  |Bowflex SelectTech 1090 Dumbbells            |599.989990234375         |
+----------+---------------------------------------------+-------------------------+


//apply filter to dataframe 
scala> daily_revenue_per_productDF.filter(daily_revenue_per_productDF("product_name") like "Nike%").show(10,truncate=false)
+----------+--------------------------------------------+-------------------------+
|order_date|product_name                                |daily_revenue_per_product|
+----------+--------------------------------------------+-------------------------+
|20130725  |Nike Men's Free 5.0+ Running Shoe           |5099.490051269531        |
|20130725  |Nike Men's CJ Elite 2 TD Football Cleat     |1949.850082397461        |
|20130725  |Nike Men's Dri-FIT Victory Golf Polo        |1650.0                   |
|20130725  |Nike Men's Kobe IX Elite Low Basketball Shoe|199.99000549316406       |
|20130725  |Nike Women's Legend V-Neck T-Shirt          |100.0                    |
|20130726  |Nike Men's Free 5.0+ Running Shoe           |6799.3199462890625       |
|20130726  |Nike Men's Dri-FIT Victory Golf Polo        |4250.0                   |
|20130726  |Nike Men's CJ Elite 2 TD Football Cleat     |3249.7501373291016       |
|20130726  |Nike Men's Comfort 2 Slide                  |134.9700050354004        |
|20130726  |Nike Dri-FIT Crew Sock 6 Pack               |110.0                    |
+----------+--------------------------------------------+-------------------------+

