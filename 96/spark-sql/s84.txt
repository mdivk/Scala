Problem Scenario 84 : In Continuation of previous question, please accomplish following activities. 
1. Select all the products which has product code as null 
2. Select all the products, whose name starts with Pen and results should be order by Price descending order. 
3. Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order. 
4. Select top 2 products by price 
===========================================================================

Solution : 
Step 1 : Select all the products which has product code as null 
val results = sqlContext.sql(“””SELECT * FROM products WHERE code IS NULL”””) 
results.show() 
val results = sqlContext.sql(“””SELECT * FROM products WHERE code = NULL”””) 
results.show() 
Step 2 : Select all the products , whose name starts with Pen and results should be order by Price descending order. 
Val results = sqlContext.sql(“””SELECT* FROM products WHERE name LIKE 'Pen%’ ORDER BY price DESC””” )
Results.show() 
Step 3 : Select all the products , whose name starts with Pen and results should be order by Price descending order and quantity ascending order
Val results = sqlContext.sql(“””SELECT* FROM products WHERE name LIKE 'Pen %' ORDER BY price DESC, quantity””” ) 
results. show() 
Step 4 : Select top 2 products by price 
val results = sqlContext.sql( """SELECT * FROM products ORDER BY price desc LIMIT 2”””) 
results.show() 
