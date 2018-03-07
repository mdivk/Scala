Problem Scenario 84 : In Continuation of previous question, please accomplish following activities. 
1. Select all the products which has product code as null 
2. Select all the products, whose name starts with Pen and results should be order by Price descending order. 
3. Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order. 
4. Select top 2 products by price 
===========================================================================

Solution : 

Step 1 : Select all the products which has product code as null 

val results11 = sqlContext.sql("SELECT * FROM products WHERE productCode IS NULL") 
results11.show() 

scala> results11.show()
+---------+-----------+----+--------+-----+-----+
|productID|productCode|name|quantity|price|image|
+---------+-----------+----+--------+-----+-----+
+---------+-----------+----+--------+-----+-----+

No result.

val results12 = sqlContext.sql("SELECT * FROM products") 
results12.show() 
+---------+-----------+--------------------+--------+------+--------------------+
|productID|productCode|                name|quantity| price|               image|
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

scala> results12.where(results12.col("productCode").isNull or results12.col("name").isNull ).show
+---------+-----------+----+--------+-----+-----+
|productID|productCode|name|quantity|price|image|
+---------+-----------+----+--------+-----+-----+
+---------+-----------+----+--------+-----+-----+

val filtered = results12.filter(row => !row.anyNull)

scala> results12.withColumn("productCode", when($"productCode".isNull, 0).otherwise(1)).count
res22: Long = 1345

results12.filter("name is null").count
res23: Long = 0

Step 2 : Select all the products , whose name starts with Pen and results should be order by Price descending order. 

Val results = sqlContext.sql("SELECT* FROM products WHERE name LIKE 'Pen%’ ORDER BY price DESC" )
Results.show() 

Step 3 : Select all the products , whose name starts with Pen and results should be order by Price descending order and quantity ascending order

Val results = sqlContext.sql("SELECT* FROM products WHERE name LIKE 'Pen %' ORDER BY price DESC, quantity" ) 
results. show() 

Step 4 : Select top 2 products by price 

val results = sqlContext.sql( """SELECT * FROM products ORDER BY price desc LIMIT 2") 
results.show() 
