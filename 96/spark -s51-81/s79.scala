Problem Scenario 79 : You have been given MySQL DB of following details. 
User=retail_dba 
password=cloudera 
database=retail_db 
table=retail_db.products 
jdbc URL = jdbc:mysql://quickstart:3306/retail_db 
Columns of products table : (product_id | product_category_id | product_name | product_description | product_price | product_image ) 
Please accomplish following activities. 
1. Copy "retail_db.products" table to hdfs in a directory p93_products 
2. Filter out all the empty prices 
3. Sort all the products based on price in both ascending as well as descending order. 
4. Sort all the products based on price as well as product_id in descending order. 
5. use the below functions to do data ordering or ranking and fetch top 10 elements 
top() 
takeordered() 
sortByKey() 




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

mysql> select count(1) from products;
+----------+
| count(1) |
+----------+
|     1345 |
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

val productsRDDMap = productsRDD.map(rec=>(rec.split(",")(0), rec.split(",")(1), rec.split(",")(2), rec.split(",")(3), rec.split(",")(4), rec.split(",")(5)))
val productsDF = productsRDDMap.toDF("product_id", "product_category_id","product_name","product_description","product_price","product_image")

productsDF.show(3)

productsDF.registerTempTable("products")
val result1 = sqlContext.sql("select * from products order by product_price limit 10")
val result2 = sqlContext.sql("select * from products order by product_price desc limit 10")
val result3 = sqlContext.sql("select * from products order by product_price desc, product_id desc limit 10")
 
result1.show
result2.show
result3.show



Step 4 : Filter empty prices, it exists 
#filter out empty prices lines 
nonempty_lines = productsRDD.filter(lambda x: len(x.split(2,”)[4])> O) 
Step 5 : Now sort data based on product_price in order. 
sortedPriceProducts=nonempty_lines.map(lambda line : (float(line.split(“,”)[4]),line.split(“,”)[2])).sortByKey() 
for line in sortedPriceProducts.collect(): 
print(line) 
Step 6 : Now sort data based on product_price in descending order. 
sortedPriceProducts=nonempty_lines.map(lambda line : (float(line.split(“,”)[4]),line.split(“,”)[2])).sortByKey(False) 
for line in sortedPriceProducts.collect(): 
print(line) 
Step 7 : Get highest price products name. 
sortedPriceProducts=nonempty_lines.map(lambda line : (float(line.split(“,”)[4]),line.split(“,”)[2])).sortByKey(False).take(1) 
print(sortedPriceProducts) 
Step 8 : Now sort data based on product_price as well as product_id in descending order. 
#Dont forget to cast string 
#Tuple as key ((price,id),name) 
sortedPriceProducts=nonempty_lines.map(lambda line : ((float(line.split(“,”)[4]),int(line.split(“,”)[0])),line.split(“,”)[2])).sortByKey(False).take(10)
print(sortedPriceProducts) 
Step 9 : Now sort data based on product_price as well as product_id in descending order, using top() function. 
#Dont forget to cast string 
#Tuple as key ((price,id),name) 
sortedPriceProducts=nonempty_lines.map(lambda line : (float(line.split(“,”)[4]),int(line.split(“,”)[0]))line.split(“,”)[2])).top(10)
print(sortedPriceProducts) 
Step 10 : Now sort data based on product_price as ascending and product_id in ascending order, using takeordered() function. 
#Dont forget to cast string 
#Tuple as key ((price,id),name) 
sortedPriceProducts=nonempty_lines.map(lambda line : (float(line.split(“,”)[4]),int(line.split(“,”)[0]))line.split(“,”)[2])).takeOrdered(10, lambda tuple : (tuple[0][0],tuple[0][1])) 
Step 11 : Now sort data based on product_price as descending and product_id in ascending order, using takeordered() function. 
#Dont forget to cast string 
#Tuple as key ((price,id),name) 
#Using minus(-) parameter can help you to make descending ordering , only for numeric value. 
sortedPriceProducts=nonempty_lines.map(lambda line : (float(line.split(“,”)[4]),int(line.split(“,”)[0])),line.split(“,”)[2])).takeOrdered(10, lambda tuple: (-tuple[0][0],tuple[0][1])) 
