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
val results1 = sqlContext.sql("SELECT concat(productCode, name) as 'product Description', price FROM products") 

org.apache.spark.sql.AnalysisException: cannot recognize input near 'as' ''product Description'' ',' in selection target; line 1 pos 41




results1.show() 


Step 3 : Select all distinct prices. 
val results = sqlContext.sql("SELECT DISTINCT price AS Distinct_Price FROM products")
results.show() 
Step 4 : Select distinct price and name combination. 
val results = sqlContext.sql(" SELECT DISTINCT price, name FROM products") 
results.show() 
Step 5 : Select all price data sorted by both code and productlD combination. 
val results = sqlContext.sql("SELECT* FROM products ORDER BY code, productiD") 
results.show() 
Step 6 : count number of products. 
val results = sqlContext.sql("SELECT COUNT(1) AS total_count FROM products") 
results.show() 
Step 7 : Count number of products for each code. 
val results = sqlContext.sql("SELECT code,COUNT(1) FROM products GROUP BY code ORDER BY count DESC") 
results.show() 
val results = sqlContext.sql("SELECT code,COUNT(1) AS count FROM products GROUP BY code ORDER BY count DESC")
results.show() 
