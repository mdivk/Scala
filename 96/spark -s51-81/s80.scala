Problem Scenario 80 : You have been given MySQL DB with following details. 

User=retail_dba 
password=cloudera 
database=retail_db 
table=retail_db.products 
jdbc URL = jdbc:mysql://quickstart:3306/retail_db 
Columns of products table : (product_id | product_category_id | product_name | product_description | product_price | product_image ) 
Please accomplish following activities. 
1. Copy "retail_db.products" table to hdfs in a directory p93_products 
2. Now sort the products data sorted by product price per category, use product_category_id colunm to group by category 

//======================================================================= 

Solution 1: Using RDD->DataFram->SparkSQL 

Step 0: Insepct the data in mysql 

mysql -h ms.itversity.com -u retail_user -p


mysql> mysql> select * from products limit 5;
+------------+---------------------+-----------------------------------------------+---------------------+---------------+---------------------------------------------------------------------------------------+
| product_id | product_category_id | product_name                                  | product_description | product_price | product_image                                                                         |
+------------+---------------------+-----------------------------------------------+---------------------+---------------+---------------------------------------------------------------------------------------+
|          1 |                   2 | Quest Q64 10 FT. x 10 FT. Slant Leg Instant U |                     |         59.98 | http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy |
|          2 |                   2 | Under Armour Men's Highlight MC Football Clea |                     |        129.99 | http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat      |
|          3 |                   2 | Under Armour Men's Renegade D Mid Football Cl |                     |         89.99 | http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat    |
|          4 |                   2 | Under Armour Men's Renegade D Mid Football Cl |                     |         89.99 | http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat    |
|          5 |                   2 | Riddell Youth Revolution Speed Custom Footbal |                     |        199.99 | http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet |
+------------+---------------------+-----------------------------------------------+---------------------+---------------+---------------------------------------------------------------------------------------+
5 rows in set (0.00 sec)

mysql> select count(1) from products where cast(product_price as decimal(10,2))>0;
+----------+
| count(1) |
+----------+
|     1338 |
+----------+
1 row in set (0.00 sec)


Step 1 : Import Single table . 


sqoop import -m 1 \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=products \
--target-dir=p93_products  


Verify the data:

[paslechoix@gw01 ~]$ hdfs dfs -tail p93_products/p*
1344,59,Nike Men's Home Game Jersey St. Louis Rams Aa,,100.0,http://images.acmesports.sports/Nike+Men%27s+Home+Game+Jersey+St.+Louis+Rams+Aaron+Donald+%2399
1345,59,Nike Men's Home Game Jersey St. Louis Rams Gr,,100.0,http://images.acmesports.sports/Nike+Men%27s+Home+Game+Jersey+St.+Louis+Rams+Greg+Robinson...


Step 3 : Load this directory as RDD using Spark and Python (Open pyspark terminal and do following). 
val productsRDD = sc.textFile("p93_products") 

//product_id | product_category_id | product_name | product_description | product_price | product_image
scala> productsRDD.first
res0: String = 1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy

//filter out empty price 
val filteredRDD = productsRDD.filter(rec=>rec.split(",")(4).toFloat>0)


val productsRDDMap = filteredRDD.map(rec=>(rec.split(",")(0).toInt, rec.split(",")(1), rec.split(",")(2), rec.split(",")(3), rec.split(",")(4).toFloat, rec.split(",")(5)))
productsRDDMap.first
res1: (String, String, String, String, String, String) = (1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,"",59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)

val productsDF = productsRDDMap.toDF("product_id", "product_category_id","product_name","product_description","product_price","product_image")

productsDF.show(3)
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|product_id|product_category_id|        product_name|product_description|product_price|       product_image|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|         1|                  2|Quest Q64 10 FT. ...|                   |        59.98|http://images.acm...|
|         2|                  2|Under Armour Men'...|                   |       129.99|http://images.acm...|
|         3|                  2|Under Armour Men'...|                   |        89.99|http://images.acm...|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
only showing top 3 rows

//2. Now sort the products data sorted by product price per category, use product_category_id colunm to group by category 

productsDF.registerTempTable("products")
val result1 = sqlContext.sql("select * from products where product_price > 0 order by product_category_id, product_price")
 
result1.show

//=======================================================================================
Solution : 
Step 1 : Import Single table.

sqoop import --connect jdbc:mysql://quickstart:3306/retail_db –username=retai_dba--password=cloudera -table=products --target-dir=p93_products –m1 
Note : Please check you dont have space between before or after ‘=’sign. 
Sqoop uses the MapReduce framework to copy data from RDBMS to hdfs 

Step 2 : Step 2 : Read the data from one of the partition, created using above command. 
hadoop fs -cat p93_products/part-m-00000 

Step 3 : Load this directory as RDD using Spark and Python (Open pyspark terminal and do following). 
productsRDD = sc.textFile("p93_products") 

Step 4 : Filter empty prices, if exists 
#filter out empty prices lines 
nonempty_lines = productsRDD.filter(lambda x:len(x.split(“,”)[4])>0) 

Step 5 : Create data set like (categroyld, (id,name,price) 
mappedRDD = nonempty_lines.map(lambda line: (line.split(",”)[1],(line.split(“,”)[2],float(line.split(“,”)[4]))) 
for line in mappedRDD.collect(): print(line) 

Step 6 : Now groupBy the all records based on categoryld, which a key on mappedRDD it will produce output like (categoryld, iterable of all lines
For akey/categoryId) 
groupByCategroyld = mappedRDD.groupByKey() 
for line in groupByCategroyid.collect(): print(line) 

Step 7 : Now sort the data in each category based on price in ascending order. 
# sorted is a function to sort an iterable, we can also specify, what would be the key on which we want to sort in this case we have price on which it needs to be sorted. 
groupByCategroyid.map(lambda tuple: sorted(tuple[l], key=lambda tupleValue: tupleValue[2])).take(5) 

Step 8 : Now sort the data in each category based on price in descending order. 
# sorted is a function to sort an iterable, we can also specify, what would be the key on which we want to sort in this case we have price on which it needs to be sorted. 
groupByCategroyid.map(lambda tuple: sorted(tuple[l], key=lambda tupleValue: tupleValue[2] , reverse=True)).take(5)
