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

No result.

val results12 = sqlContext.sql("SELECT * FROM products") 
results12.show() 

scala> results.where(results.col("productCode").isNull or results.col("name").isNull ).show

val filtered = results.filter(row -> !row.anyNull())
results.withColumn("productCode", when($"productCode".isNull, 0).otherwise(1)).show
results.filter("name is null").show


Step 2 : Select all the products , whose name starts with Pen and results should be order by Price descending order. 

Val results = sqlContext.sql("SELECT* FROM products WHERE name LIKE 'Pen%’ ORDER BY price DESC" )
Results.show() 

Step 3 : Select all the products , whose name starts with Pen and results should be order by Price descending order and quantity ascending order

Val results = sqlContext.sql("SELECT* FROM products WHERE name LIKE 'Pen %' ORDER BY price DESC, quantity" ) 
results. show() 

Step 4 : Select top 2 products by price 

val results = sqlContext.sql( """SELECT * FROM products ORDER BY price desc LIMIT 2") 
results.show() 
