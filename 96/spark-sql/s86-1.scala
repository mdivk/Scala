Problem Scenario 86 : In Continuation ot previous question, please accomplish following activities. 
1.Select Maximum, minimum, average , Standard Deviation, and total quantity. 
2. Select minimum and maximum price for each product code. 
3. Select Maximum, minimum, average , Standard Deviation, and total quantity for each product code, hwoever make sure Average and Standard deviation will have maximum two decimal values. 
4. Select all the product code and average price only where product count is more than or equal to 3. 
5. Select maximum, minimum , average and total of all the products for each code. Also produce the same across all the products. 

Solution : 

//Prepare the temp table
//1. Loading data

scala> val prd = sc.textFile("data/product.csv")
scala> val stage = prd.map(p=>p.split(","))
scala> val prdRDD = stage.map(p=>(p(0).toInt, p(1), p(2), p(3).toInt, { if( p(4)==null ||  p(4)=="" ) 0 else p(4).toFloat }))
scala> val prdDF = prdRDD.toDF("productID","productCode","name","quantity","price")


//2. generate DF and register temp table 
scala> prdDF.registerTempTable("products")


val raw = sqlContext.sql("select * from products")
raw.show
+---------+-----------+---------+--------+-------+
|productlD|productCode|     name|quantity|  price|
+---------+-----------+---------+--------+-------+
|     1001|        PEN|  pen Red|    5000|      1|
|     1002|        PEN| pen Blue|    8000|      1|
|     1003|        PEN|pen Black|    8000|      1|
|     1004|        PEC|Pencil 2B|   10000|   0.48|
|     1005|        PEC|Pencil 2H|    8000|   0.49|
|     1004|        PEC|Pencil HB|       0|9999.99|
+---------+-----------+---------+--------+-------+


Step 1 : Select Maximum, minimum, average , Standard Deviation, and total quantity. 
val results = sqlContext.sql("SELECT MAX(price) AS MAX , MIN(price) AS MIN , AVG(price) AS Average, STD(price) AS STD, SUM(quantitY) AS total_products From products")
results. show() 
+-------+----+------------------+-----------------+--------------+
|    MAX| MIN|           Average|              STD|total_products|
+-------+----+------------------+-----------------+--------------+
|9999.99|0.48|1667.3266666666666|3726.480336509089|       39000.0|
+-------+----+------------------+-----------------+--------------+

+-------+----+-----------------+------------------+--------------+
|    MAX| MIN|          Average|               STD|total_products|
+-------+----+-----------------+------------------+--------------+
|9999.99|0.48|1667.326705728968|3726.4804238555826|         39000|
+-------+----+-----------------+------------------+--------------+



Step 2 : Select minimum and maximum price tor each product code. 
val results2 = sqlContext.sql("SELECT productCode, MAX(price) AS Highest_Price, MIN(price) AS Lowest_Price FROM products GROUP BY productCode") 
results2.show() 
+-----------+-------------+------------+
|productCode|Highest_Price|Lowest_Price|
+-----------+-------------+------------+
|        PEC|      9999.99|        0.48|
|        PEN|            1|           1|
+-----------+-------------+------------+

Step 3 : Select Maximum, minimum, average , Standard Deviation, and total quantity for each product code, hwoever make sure Average and stand deviation will have maximum two decimal values. 
val results3 = sqlContext.sql("SELECT productCode, MAX(price) as MAX_Price, MIN(price) as MIN_Price, CAST(AVG(price) As DECIMAL(7,2)) AS AVERAGE, CAST(STD(price) As DECIMAL(7,2)) AS Std_Dev, SUM(quantity) as Total FROM products GROUP BY productCode") 
results3.show() 
+-----------+---------+---------+-------+-------+-------+
|productCode|MAX_Price|MIN_Price|AVERAGE|Std_Dev|  Total|
+-----------+---------+---------+-------+-------+-------+
|        PEC|  9999.99|     0.48|3333.65|4713.81|18000.0|
|        PEN|        1|        1|   1.00|   0.00|21000.0|
+-----------+---------+---------+-------+-------+-------+



Step 4 : Select all the product code and average price only where product count is more than or equal to 3. 
val results4 = sqlContext.sql( "SELECT productCode, COUNT(1) AS cnt, CAST(AVG(Price) AS DECIMAL(7,2)) AS Average FROM products GROUP BY productCode HAVING cnt >=3") 
results4.show() 
+-----------+---+-------+
|productCode|cnt|Average|
+-----------+---+-------+
|        PEC|  3|3333.65|
|        PEN|  3|   1.00|
+-----------+---+-------+

Step 5 : Select maximum, minimum , average and total ot all the products tor each code. Also produce the same across all the products. 
val results5 = sqlContext.sql( "SELECT productCode, MAX(price) as MAX_Price, MIN(price) as MIN_Price, CAST(AVG(PRICE) AS DECIMAL(7,2)) AS Average, SUM(quantity) as Total FROM products GROUP BY productCode WITH ROLLUP") 
results5. show()
+-----------+---------+---------+-------+-------+
|productCode|MAX_Price|MIN_Price|Average|  Total|
+-----------+---------+---------+-------+-------+
|       null|  9999.99|     0.48|1667.33|39000.0|
|        PEC|  9999.99|     0.48|3333.65|18000.0|
|        PEN|        1|        1|   1.00|21000.0|
+-----------+---------+---------+-------+-------+

Step 6 : Select maximum, minimum , average and total ot all the products tor each code. Also produce the same across all the products. 
val results6 = sqlContext.sql( "SELECT productCode, MAX(price) as MAX_Price, MIN(price) as MIN_Price, CAST(AVG(PRICE) AS DECIMAL(7,2)) AS Average, SUM(quantity) as Total FROM products GROUP BY productCode") 
results6. show()
+-----------+---------+---------+-------+-------+
|productCode|MAX_Price|MIN_Price|Average|  Total|
+-----------+---------+---------+-------+-------+
|        PEC|  9999.99|     0.48|3333.65|18000.0|
|        PEN|        1|        1|   1.00|21000.0|
+-----------+---------+---------+-------+-------+
