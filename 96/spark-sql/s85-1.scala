Problem Scenario 85 : In Continuation of previous question, please accomplish following activities. 
1. Select all the columns from product table with output header as below. 
productiD AS ID 
code AS Code 
name AS Description 
price AS ‘unit Price' 
2. Select code and name both separated by ' - ' and header name should be 'product Description'. 
3. Select all distinct prices. 
4. Select distinct price and name combination. 
5. Select all price data sorted by both code and productlD combination. 

6. count number of products. 
7. Count number of products for each code. 
=================================================================== 
Solution : 
Step 1 : Select all the columns from product table with output header as below. 
productiD AS ID 
code AS Code 
name AS Description 
price AS 'unit Price' 

[productID: int, productCode: string, name: string, quantity: string, price: float, image: string]



scala> val prd = sc.textFile("products")
scala> val stage = prd.map(p=>p.split(","))
scala> val prdRDD = stage.map(p=>(p(0).toInt, p(1).toInt, p(2), p(3), { if( p(4)==null ||  p(4)=="" ) 0 else p(4).toFloat },p(5)))
scala> val prdDF = prdRDD.toDF("productID","productCode","name","quantity","price", "image")
prdDF.registerTempTable("products")

val results = sqlContext.sql("SELECT productID AS ID, productCode AS code, name AS Description, price AS unit_price FROM products ORDER BY ID") 
results.show() 
+---+----+--------------------+----------+
| ID|code|         Description|unit_price|
+---+----+--------------------+----------+
|  1|   2|Quest Q64 10 FT. ...|     59.98|
|  2|   2|Under Armour Men'...|    129.99|
|  3|   2|Under Armour Men'...|     89.99|
|  4|   2|Under Armour Men'...|     89.99|
|  5|   2|Riddell Youth Rev...|    199.99|
|  6|   2|Jordan Men's VI R...|    134.99|
|  7|   2|Schutt Youth Recr...|     99.99|
|  8|   2|Nike Men's Vapor ...|    129.99|
|  9|   2|Nike Adult Vapor ...|      50.0|
| 10|   2|Under Armour Men'...|    129.99|
| 11|   2|Fitness Gear 300 ...|    209.99|
| 12|   2|Under Armour Men'...|    139.99|
| 13|   2|Under Armour Men'...|     89.99|
| 14|   2|Quik Shade Summit...|    199.99|
| 15|   2|Under Armour Kids...|     59.99|
| 16|   2|Riddell Youth 360...|    299.99|
| 17|   2|Under Armour Men'...|    129.99|
| 18|   2|Reebok Men's Full...|     29.97|
| 19|   2|Nike Men's Finger...|    124.99|
| 20|   2|Under Armour Men'...|    129.99|
+---+----+--------------------+----------+


Step 2 : Select code and name both separated by ' - ' and header name should be 'product Description'. 
val results1 = sqlContext.sql("SELECT concat(productCode, name) as Product_Description, price FROM products") 
results1.show() 
+--------------------+------+
| Product_Description| price|
+--------------------+------+
|2Quest Q64 10 FT....| 59.98|
|2Under Armour Men...|129.99|
|2Under Armour Men...| 89.99|
|2Under Armour Men...| 89.99|
|2Riddell Youth Re...|199.99|
|2Jordan Men's VI ...|134.99|
|2Schutt Youth Rec...| 99.99|
|2Nike Men's Vapor...|129.99|
|2Nike Adult Vapor...|  50.0|
|2Under Armour Men...|129.99|
|2Fitness Gear 300...|209.99|
|2Under Armour Men...|139.99|
|2Under Armour Men...| 89.99|
|2Quik Shade Summi...|199.99|
|2Under Armour Kid...| 59.99|
|2Riddell Youth 36...|299.99|
|2Under Armour Men...|129.99|
|2Reebok Men's Ful...| 29.97|
|2Nike Men's Finge...|124.99|
|2Under Armour Men...|129.99|
+--------------------+------+
only showing top 20 rows



Step 3 : Select all distinct prices. 
val results = sqlContext.sql("SELECT DISTINCT price AS Distinct_Price FROM products")
results.show() 
+--------------+
|Distinct_Price|
+--------------+
|         108.0|
|        179.98|
|         12.99|
|         99.98|
|         29.99|
|          25.0|
|           8.0|
|          72.0|
|         241.0|
|         63.99|
|         59.98|
|          50.0|
|         169.0|
|         219.0|
|         194.0|
|        139.99|
|        189.99|
|         81.99|
|          10.8|
|          75.0|
+--------------+
only showing top 20 rows


Step 4 : Select distinct price and name combination. 
val results = sqlContext.sql(" SELECT DISTINCT price, name FROM products") 
results.show() 

Step 5 : Select all price data sorted by both code and productlD combination. 
val results = sqlContext.sql("SELECT* FROM products ORDER BY code, productiD") 
results.show() 
+------+--------------------+
| price|                name|
+------+--------------------+
| 24.99|Under Armour Men'...|
|  79.0|lucy Women's Hath...|
| 34.99|Cutters Adult C-T...|
|  18.0|adidas Men's 2014...|
|249.97|Easton Mako Youth...|
|199.99|Nike Men's LeBron...|
|  32.0|Nike Men's St. Lo...|
| 19.99|Glove It Women's ...|
| 19.99|Glove It Women's ...|
|399.98|Field & Stream Sp...|
| 34.97|Nike Men's Chicag...|
|  26.0|Reebok Men's Toro...|
| 29.99|Perfect Multi-Gym...|
|179.99|Schutt XV Flex Sk...|
|159.99|Rollerblade Men's...|
| 21.99|Nike Men's Benass...|
| 19.99|Glove It Women's ...|
| 51.99|Titleist Pro V1 P...|
|299.99|Nishiki Adult Mon...|
|399.99|Teeter Hang-Ups E...|
+------+--------------------+
only showing top 20 rows


Step 6 : count number of products. 
val results = sqlContext.sql("SELECT COUNT(1) AS total_count FROM products") 
results.show() 

Step 7 : Count number of products for each code. 

val results = sqlContext.sql("SELECT productCode,COUNT(1) AS count FROM products GROUP BY productCode ORDER BY count DESC")
results.show() 
+-----------+-----+
|productCode|count|
+-----------+-----+
|         38|   48|
|         41|   48|
|         54|   24|
|         31|   24|
|         43|   24|
|         37|   24|
|         36|   24|
|         40|   24|
|         33|   24|
|         52|   24|
|         35|   24|
|         34|   24|
|         44|   24|
|         32|   24|
|         45|   24|
|         39|   24|
|         47|   24|
|         46|   24|
|         56|   24|
|         48|   24|
+-----------+-----+
only showing top 20 rows


