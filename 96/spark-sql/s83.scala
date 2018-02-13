Problem Scenario 83 : In Continuation of previous question, please accomplish following activities. 
1. Select all the records with quantity >= 5000 and name starts with 'Pen' 
2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen' 
3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen' 
4. Select all the products which name is 'Pen Red', 'Pen Black' 
5. select all the products WhiCh has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000. 

Solution : 
Step 1 : Select all the records with quantity >= 5000 and name starts with 'Pen' 
val results = sqcontext.sql("SELECT * FROM products WHERE quantity >= 5000 AND name LIKE Pen%") 
results.show() 
Step 2 : Select all the records with quantity >=5000 , price is less than 1.24 and name starts with 'Pen' 
val results = sqcontext.sql( "'"'SELECT* FROM products WHERE quantity >= 5000 AND price < 1.24 AND name LIKE •pen%’ """)
results.show() 
Step 3 : Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen' 
val results = sqcontext.sql(""" SELECT* FROM products WHERE NOT (quantity >=5000 AND name LIKE •pen %)""") 
results.show() 
Step 4 : Select all the products wchich name is 'Pen Red', 'Pen Black' 
val results = sqcontext.sql( """SELECT* FROM products WHERE name IN (‘pen Red', 'Pen Black') """ ) 
results.show() 
step 5 : select all the products WhiCh has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000. 
val results = sqcontext.sql(""" SELECT* FROM products WHERE (price BETWEEN 1.0 AND 2.0) AND (quantity BETWEEN 1000 AND 2000) """) 
results.show() 
