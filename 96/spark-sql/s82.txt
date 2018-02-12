Problem Scenario 82 : You have been given table in Hive with following structure (Which you have created in previous exercise). 
productid int 
code string 
name string 
quantity int 
price float 
using SparkSQL accomplish following activities. 
1. Select all the products name and quantity having quantity <=2000 
2. Select name and price ot the product having code as 'PEN' 
3. Select all the products, which name starts with PENCIL 
4. Select all products which "name" begins with 'P', followed by any two characters, followed by space, followed by zero or more characters 

Solution . 

Preparation:

scala> val prdRDD = products.map(p=>(p.split(",")(0), p.split(",")(1), p.split(",")(2), p.split(",")(3), p.split(",")(4)))
prdRDD: org.apache.spark.rdd.RDD[(String, String, String, String, String)] = MapPartitionsRDD[4] at map at <console>:29

scala> val prdDF = prdRDD.toDF("productlD","productCode","name","quantity","price")
prdDF: org.apache.spark.sql.DataFrame = [productlD: string, productCode: string, name: string, quantity: string, price: string]

scala> prdDF.show
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

scala> prdDF.registerTempTable("products")



Step 3 : Select all the products name and quantity having quantity<=2000 

scala> val result1 = sqlContext.sql("select name, quantity from products where quantity <=2000")
results.show() 
+---------+--------+
|     name|quantity|
+---------+--------+
|Pencil HB|       0|
+---------+--------+


Step 4 : Select name and price of the product having code as 'PEN' 
scala> val result2 = sqlContext.sql("select productCode, name, quantity from products where productCode == 'PEN'")
results2.show() 

+-----------+---------+--------+
|productCode|     name|quantity|
+-----------+---------+--------+
|        PEN|  pen Red|    5000|
|        PEN| pen Blue|    8000|
|        PEN|pen Black|    8000|
+-----------+---------+--------+

Step 5 : Select all the products , which name starts with PENCIL 
val results3 = sqlContext.sql(" SELECT name, price FROM products WHERE upper(name) LIKE 'PENCIL%' ") 
results.show() 
+---------+-------+
|     name|  price|
+---------+-------+
|Pencil 2B|   0.48|
|Pencil 2H|   0.49|
|Pencil HB|9999.99|
+---------+-------+

Step 6 : select all products which "name" begins with 'P', followed by any two characters, followed by space, followed by zero or more characters
--"name" begins with 'P', followed by any two characters, 
--followed by space, followed by zero or more characters 
//Question is not clear

val results4 = sqlContext.sql(" SELECT name, price FROM products WHERE upper(name) LIKE 'P_ %' ") 
results4.show() 
