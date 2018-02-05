Problem Scenario 86 : In Continuation ot previous question, please accomplish following activities. 
1.Select Maximum, minimum, average , Standard Deviation, and total quantity. 
2. Select minimum and maximum price for each product code. 
3. Select Maximum, minimum, average , Standard Deviation, and total quantity for each product code, hwoever make sure Average and Standard deviation will have maximum two decimal values. 
4. Select all the product code and average price only where product count is more than or equal to 3. 
5. Select maximum, minimum , average and total of all the products for each code. Also produce the same across all the products. 

Solution : 
Step 1 : Select Maximum, minimum, average , Standard Deviation, and total quantity. 
val results = sqcontext.sql( “""SELECT MAX(price) AS MAX , MIN(price) AS 
MIN , AVG(price) AS Average, STD(price) AS STD, SUM(quantitY) AS total_products From products””” )
results. show() 

Step 2 : Select minimum and maximum price tor each product code. 
val results = sqlContext.sql( """ SELECT code, MAX(price) AS •Highest Price', MIN(price) AS •Lowest Price' 
FROM products 
GROUP BY code""") 
results.show() 
Step 3 : Select Maximum, minimum, average , Standard Deviation, and total quantity for each product code, hwoever make sure Average and stand deviation will have maximum two decimal values. 
val results = sqlContext.sql( """ SELECT code, MAX(price), MIN(price), 
CAST(AVG(price) As DECIMAL(7,2)) AS ‘AVERAGE’, 
CAST(STD(price) As DECIMAL(7,2)) AS ‘Std Dev’,
SUM(quantity) 
FROM products 
GROUP BY code """) 
results.show() 
Step 4 : Select all the product code and average price only where product count is more than or equal to 3. 
val results = sqlContext.sql( """SELECT code AS •product Code' , 
COUNT(*)AS •count', 
CAST(AVG(Price)AS DECIMAL(7,2))AS 'Average 
FROM products 
GROUP BY code
HAVING count >=3""" ) 
results. show() 

Step 5 : Select maximum, minimum , average and total ot all the products tor each code. Also produce the same across all the products. 
val results = sqlContext.sql( """ SELECT 
code, 
MAX(price), 
MIN(price), 
CAST(AVG(PRICE) AS DECIMAL(7,2)AS'Average , 
SUM(quantity) 
FROM products  
GROUP BY code 
WITH ROLLUP'""' ) 
results. show()
