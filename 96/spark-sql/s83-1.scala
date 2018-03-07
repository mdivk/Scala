Problem Scenario 83 : In Continuation of previous question, please accomplish following activities. 
1. Select all the records with quantity >= 5000 and name starts with 'Pen' 
2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen' 
3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen' 
4. Select all the products which name is 'Pen Red', 'Pen Black' 
5. select all the products WhiCh has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000. 

Solution : 

 
1. There are 1345 records in products in mysql, export them to hdfs:

mysql> show create table products;
+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table    | Create Table                                                                                                                                                                                                                                                                                                                                                               |
+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| products | CREATE TABLE `products` (
  `product_id` int(11) NOT NULL AUTO_INCREMENT,
  `product_category_id` int(11) NOT NULL,
  `product_name` varchar(45) NOT NULL,
  `product_description` varchar(255) NOT NULL,
  `product_price` float NOT NULL,
  `product_image` varchar(255) NOT NULL,
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1346 DEFAULT CHARSET=utf8 |
+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

mysql> select count(1) from products;
+----------+
| count(1) |
+----------+
|     1345 |
+----------+
1 row in set (0.01 sec)


sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=products \
--target-dir=products 


[paslechoix@gw03 ~]$ hdfs dfs -ls products
Found 2 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-05 06:54 products/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     173993 2018-03-05 06:54 products/part-m-00000

hdfs dfs -tail products/part-m-00000
1340,59,Majestic Men's Replica Texas Rangers Russell ,,69.97,http://images.acmesports.sports/Majestic+Men%27s+Replica+Texas+Rangers+Russell+Wilson+%233+Home...
1341,59,Nike Women's Cleveland Browns Johnny Football,,34.0,http://images.acmesports.sports/Nike+Women%27s+Cleveland+Browns+Johnny+Football+Orange+T-Shirt
1342,59,Nike Men's St. Louis Rams Michael Sam #96 Nam,,32.0,http://images.acmesports.sports/Nike+Men%27s+St.+Louis+Rams+Michael+Sam+%2396+Name+and+Number...
1343,59,Nike Men's Home Game Jersey St. Louis Rams Mi,,100.0,http://images.acmesports.sports/Nike+Men%27s+Home+Game+Jersey+St.+Louis+Rams+Michael+Sam+%2396
1344,59,Nike Men's Home Game Jersey St. Louis Rams Aa,,100.0,http://images.acmesports.sports/Nike+Men%27s+Home+Game+Jersey+St.+Louis+Rams+Aaron+Donald+%2399
1345,59,Nike Men's Home Game Jersey St. Louis Rams Gr,,100.0,http://images.acmesports.sports/Nike+Men%27s+Home+Game+Jersey+St.+Louis+Rams+Greg+Robinson...


2. Create RDD based on the hdfs file of products
scala> val prd = sc.textFile("products")
res0: Long = 1345

scala> val prdRDD = prd.map(p=>(p.split(",")(0).toInt, p.split(",")(1), p.split(",")(2), p.split(",")(3), p.split(",")(4).toFloat,p.split(",")(5)))

Note: the above prdRDD will fail later, check in mysql and found there are some records with price is empty, this will cause .toFloat error out.

It should be re-written as either of below:

scala> val prdMap = prd.map(p=>(p.split(",")(0).toInt,p.split(",")(1).toInt, p.split(",")(2), p.split(",")(3), { if( p.split(",")(4)==null ||  p.split(",")(4)=="" ) 0 else p.split(",")(4).toFloat }, p.split(",")(5)))
res9: (Int, Int, String, String, Float, String) = (1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,"",59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)


val stage = prd.map(p=>p.split(","))
res5: Array[Array[String]] = Array(
  Array(1, 2, Quest Q64 10 FT. x 10 FT. Slant Leg Instant U, "", 59.98, http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy), 
  Array(2, 2, Under Armour Men's Highlight MC Football Clea, "", 129.99, http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat), 
  Array(3, 2, Under Armour Men's Renegade D Mid Football Cl, "", 89.99, http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat), 
  Array(4, 2, Under Armour Men's Renegade D Mid Football Cl, "", 89.99, http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat), 
  Array(5, 2, Riddell Youth Revolution Speed Custom Footbal, "", 199.99, http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom...
continue with a second map, this way saves some repetitive split
scala> val prdRDD = stage.map(p=>(p(0).toInt, p(1).toInt, p(2), p(3), { if( p(4)==null ||  p(4)=="" ) 0 else p(4).toFloat },p(5)))
res11: (Int, Int, String, String, Float, String) = (1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,"",59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)

It is the same as the previous RDD 




scala> prdRDD
res12: org.apache.spark.rdd.RDD[(Int, String, String, String, Float, String)] = MapPartitionsRDD[40] at map at <console>:29\


scala> val prdDF = prdRDD.toDF("productID","productCode","name","quantity","price", "image")
prdDF.registerTempTable("products")

val result = sqlContext.sql("select * from products")
result.show
+---------+-----------+--------------------+--------+------+--------------------+
|productlD|productCode|                name|quantity| price|               image|
+---------+-----------+--------------------+--------+------+--------------------+
|        1|          2|Quest Q64 10 FT. ...|        | 59.98|http://images.acm...|
|        2|          2|Under Armour Men'...|        |129.99|http://images.acm...|
|        3|          2|Under Armour Men'...|        | 89.99|http://images.acm...|
|        4|          2|Under Armour Men'...|        | 89.99|http://images.acm...|
|        5|          2|Riddell Youth Rev...|        |199.99|http://images.acm...|
|        6|          2|Jordan Men's VI R...|        |134.99|http://images.acm...|
|        7|          2|Schutt Youth Recr...|        | 99.99|http://images.acm...|
|        8|          2|Nike Men's Vapor ...|        |129.99|http://images.acm...|
|        9|          2|Nike Adult Vapor ...|        |  50.0|http://images.acm...|
|       10|          2|Under Armour Men'...|        |129.99|http://images.acm...|
|       11|          2|Fitness Gear 300 ...|        |209.99|http://images.acm...|
|       12|          2|Under Armour Men'...|        |139.99|http://images.acm...|
|       13|          2|Under Armour Men'...|        | 89.99|http://images.acm...|
|       14|          2|Quik Shade Summit...|        |199.99|http://images.acm...|
|       15|          2|Under Armour Kids...|        | 59.99|http://images.acm...|
|       16|          2|Riddell Youth 360...|        |299.99|http://images.acm...|
|       17|          2|Under Armour Men'...|        |129.99|http://images.acm...|
|       18|          2|Reebok Men's Full...|        | 29.97|http://images.acm...|
|       19|          2|Nike Men's Finger...|        |124.99|http://images.acm...|
|       20|          2|Under Armour Men'...|        |129.99|http://images.acm...|
+---------+-----------+--------------------+--------+------+--------------------+
only showing top 20 rows



Step 1 : Select all the records with name starts with 'Per' 

val result1 = sqlContext.sql("select * from products where name like 'Per%'")

result1.show

18/03/05 20:35:16 INFO HadoopRDD: Input split: hdfs://nn01.itversity.com:8020/user/paslechoix/products/part-m-00000:0+86996
18/03/05 20:35:16 ERROR Executor: Exception in task 0.0 in stage 14.0 (TID 15)
java.lang.NumberFormatException: empty String
        at sun.misc.FloatingDecimal.readJavaFormatString(FloatingDecimal.java:1842)
        at sun.misc.FloatingDecimal.parseFloat(FloatingDecimal.java:122)
        at java.lang.Float.parseFloat(Float.java:451)

But if restrain the result1 to show 7 or less, then it works:

scala> result1.show(7)

+---------+-----------+--------------------+--------+-----+--------------------+
|productID|productCode|                name|quantity|price|               image|
+---------+-----------+--------------------+--------+-----+--------------------+
|      362|         17|Perfect Fitness P...|        |29.99|http://images.acm...|
|      365|         17|Perfect Fitness P...|        |59.99|http://images.acm...|
|      372|         17|Perfect Ab Carver...|        |39.99|http://images.acm...|
|      373|         17|Perfect Fitness M...|        |39.99|http://images.acm...|
|      374|         17|Perfect Pushup BASIC|        |19.99|http://images.acm...|
|      376|         17|Perfect Pushup V2...|        |29.99|http://images.acm...|
|      377|         17|Perfect Pullup Basic|        |19.99|http://images.acm...|
+---------+-----------+--------------------+--------+-----+--------------------+
only showing top 7 rows

check in mysql, there are 12 records:

mysql> select * from products where product_name like 'Per%' and NOT (product_category_id = 17 and product_id = 379);
+------------+---------------------+--------------------------------------------+---------------------+---------------+----------------------------------------------------------------------------+
| product_id | product_category_id | product_name                               | product_description | product_price | product_image                                                              |
+------------+---------------------+--------------------------------------------+---------------------+---------------+----------------------------------------------------------------------------+
|        362 |                  17 | Perfect Fitness Perfect Ab Strap Pro       |                     |         29.99 | http://images.acmesports.sports/Perfect+Fitness+Perfect+Ab+Strap+Pro       |
|        365 |                  17 | Perfect Fitness Perfect Rip Deck           |                     |         59.99 | http://images.acmesports.sports/Perfect+Fitness+Perfect+Rip+Deck           |
|        372 |                  17 | Perfect Ab Carver Pro                      |                     |         39.99 | http://images.acmesports.sports/Perfect+Ab+Carver+Pro                      |
|        373 |                  17 | Perfect Fitness Multi Gym Pro              |                     |         39.99 | http://images.acmesports.sports/Perfect+Fitness+Multi+Gym+Pro              |
|        374 |                  17 | Perfect Pushup BASIC                       |                     |         19.99 | http://images.acmesports.sports/Perfect+Pushup+BASIC                       |
|        376 |                  17 | Perfect Pushup V2 Performance              |                     |         29.99 | http://images.acmesports.sports/Perfect+Pushup+V2+Performance              |
|        377 |                  17 | Perfect Pullup Basic                       |                     |         19.99 | http://images.acmesports.sports/Perfect+Pullup+Basic                       |
|        379 |                  17 | Perfect Multi-Gym - As Seen on TV!         |                     |         29.99 | http://images.acmesports.sports/Perfect+Multi-Gym+-+As+Seen+on+TV%21       |
|       1013 |                  46 | Perception Sport Swifty Deluxe 9.5 Kayak   |                     |        349.99 | http://images.acmesports.sports/Perception+Sport+Swifty+Deluxe+9.5+Kayak   |
|       1030 |                  46 | Perception Sport Striker 11.5 Angler Kayak |                     |        499.99 | http://images.acmesports.sports/Perception+Sport+Striker+11.5+Angler+Kayak |
|       1065 |                  48 | Perception Sport Swifty Deluxe 9.5 Kayak   |                     |        349.99 | http://images.acmesports.sports/Perception+Sport+Swifty+Deluxe+9.5+Kayak   |
|       1093 |                  49 | Perception Sport Swifty Deluxe 9.5 Kayak   |                     |        349.99 | http://images.acmesports.sports/Perception+Sport+Swifty+Deluxe+9.5+Kayak   |
+------------+---------------------+--------------------------------------------+---------------------+---------------+----------------------------------------------------------------------------+

val result2 = sqlContext.sql("select * from products where productID = 379")

result2.show(1)


val result2 = sqlContext.sql("select * from products where productID = 377")

result2.show(1)

java.lang.NumberFormatException: empty String


Now, skip the record:
val result3 = sqlContext.sql("select * from products where name like 'Per%' and (NOT (productCode = 17 and productlD = 379)"))
result3.show

val result4 = sqlContext.sql("select * from products where productID != 379 and productID !=377")
result4.show(1)


Troubleshooting:

When there is no query in the sql, show won't produce error; 
When there is any where clause in the sql, because show will cause the action and the original toFloat will fail due to some records have price column value being empty or null(in this case just empty)

This causes the show crashes.

to validate the suspect, check in mysql with query below:

mysql> select * from products where product_price is null  or product_price = '';

+------------+---------------------+-----------------------------------------------+---------------------+---------------+-----------------------------------------------------------------------------------------+
| product_id | product_category_id | product_name                                  | product_description | product_price | product_image                                                                           |
+------------+---------------------+-----------------------------------------------+---------------------+---------------+-----------------------------------------------------------------------------------------+
|         38 |                   3 | Nike Men's Hypervenom Phantom Premium FG Socc |                     |             0 | http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat |
|        388 |                  18 | Nike Men's Hypervenom Phantom Premium FG Socc |                     |             0 | http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat |
|        414 |                  19 | Nike Men's Hypervenom Phantom Premium FG Socc |                     |             0 | http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat |
|        517 |                  24 | Nike Men's Hypervenom Phantom Premium FG Socc |                     |             0 | http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat |
|        547 |                  25 | Nike Men's Hypervenom Phantom Premium FG Socc |                     |             0 | http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat |
|        934 |                  42 | Callaway X Hot Driver                         |                     |             0 | http://images.acmesports.sports/Callaway+X+Hot+Driver                                   |
|       1284 |                  57 | Nike Men's Hypervenom Phantom Premium FG Socc |                     |             0 | http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat |
+------------+---------------------+-----------------------------------------------+---------------------+---------------+-----------------------------------------------------------------------------------------+
7 rows in set (0.00 sec)

mysql>


In mysql, the columns are filled with value of 0 so that they will get output instead of crashes.



Step 2 : Select all the records with quantity >=5000 , price is less than 100 and name starts with 'Per' 

val result2 = sqlContext.sql( "SELECT * FROM products WHERE price < 100")
result2.show() 

Note: the data source has some issue and needs to be sanitized before it stops throwing error.

Troublehooting:
1. find out the source data on hdfs, load it into a new hive table
2. check the hive table to find out if there are extra malformed records like null in any columns

To create hive table:
hdfs dfs -cp /public/retail_db/products/part-00000 .

create external table products_hive (product_id int, product_category_id int, product_name string, product_description string, product_price decimal(8,2), product_image string)
row format delimited fields terminated by ','
location '/user/paslechoix/products/';

1345
Time taken: 50.443 seconds, Fetched: 1 row(s)
hive (paslechoix)>
