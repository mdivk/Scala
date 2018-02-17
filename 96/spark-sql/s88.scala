Problem Scenario 88 : You have been given below three Files 

Attention: 
1. it is very important to ensure the ending of each line in the csv has no space trailing
2. no quoted column name is allowed, i.e. product's name must not be 'product Name', must be productName, in one word


product.csv (Create this File in hdfs) 

productlD,productCode,name,quantity,price,supplierid 
1001,PEN,pen Red,5000,1.23,501 
1002,PEN,pen blue,8000,1.25,501
1003,PEN,pen Black,2000,1.25,501
1004,PEC,pencil 2B,10000,0.48,502
1005,PEC,pencil 2H,8000,0.49,502
1006,PEC,pencil 2B,10000,0.48,502
2001,PEC,pencil 3B,5000,0.52,501
2002,PEC,pencil 4B,200,0.62,501
2003,PEC,pencil 5B,100,0.73,501
2004,PEC,pencil 6B,500,0.47,502

[paslechoix@gw03 ~]$ hdfs dfs -cat product.csv
1001,PEN,pen Red,5000,1.23,501
1002,PEN,pen blue,8000,1.25,501
1003,PEN,pen Black,2000,1.25,501
1004,PEC,pencil 2B,10000,0.48,502
1005,PEC,pencil 2H,8000,0.49,502
1006,PEC,pencil 2B,10000,0.48,502
2001,PEC,pencil 3B,5000,0.52,501
2002,PEC,pencil 4B,200,0.62,501
2003,PEC,pencil 5B,100,0.73,501
2004,PEC,pencil 6B,500,0.47,502

supplier.csv 

supplierid,name,phone 
501,XYZ company,88882222 
503,QQ corp,88883333 

[paslechoix@gw03 ~]$ hdfs dfs -cat supplier.csv
501,XYZ company,88882222
503,QQ corp,88883333


products_suppliers.csv 

productlD,supplierlD_ 
2001,501 
2002,501 
2003,501 
2004,502 
2001,503

[paslechoix@gw03 ~]$ hdfs dfs -cat products_suppliers.csv
2001,501
2002,501
2003,501
2004,502
2001,503


Now accomplish all the queries given in solution. 

1. It is possible that, same product can be supplied by multiple supplier. Now find each product , its price according to each supplier.  (group by product, and sorted by supplier)
2. Find all the supllier name, who are supplying 'Pencil 3B' 
3. Find all the products , which are supplied by ABC Traders. 

Solution : 

Step 0:
Create RDD, convert to DF, register temp table
val productRDD = sc.textFile("product.csv")
val productRDDMap = productRDD.map(x=>(x.split(",")(0).toInt, x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4), x.split(",")(5).toInt))

val productDF = productRDDMap.toDF("productID","productCode","name","quantity","price","supplierid")
productDF.registerTempTable("product")

val supplierRDD = sc.textFile("supplier.csv").map(x=>(x.split(",")(0).toInt, x.split(",")(1), x.split(",")(2)))
val supplierDF = supplierRDD.toDF("supplierid","name","phone")
supplierDF.registerTempTable("supplier")

val products_suppliersRDD = sc.textFile("products_suppliers.csv").map(x=>(x.split(",")(0).toInt, x.split(",")(1).toInt))
val products_suppliersDF = products_suppliersRDD.toDF("productID","supplierID")
products_suppliersDF.registerTempTable("products_suppliers")



Step 1 : It is possible that , same product can be supplied by multiple supplier. Now tind each product , its price according to each supplier. 

val results = sqlContext.sql( "SELECT product.name AS productName, product.price, supplier.name AS supplierName FROM products_suppliers JOIN product ON products_suppliers.productID = product.productID JOIN supplier ON products_suppliers.supplierID = supplier.supplierID") 
results.show() 

+-----------+-----+------------+
|productName|price|supplierName|
+-----------+-----+------------+
|  pencil 3B| 0.52| XYZ company|
|  pencil 4B| 0.62| XYZ company|
|  pencil 5B| 0.73| XYZ company|
|  pencil 3B| 0.52|     QQ corp|
+-----------+-----+------------+




Step 2 : Find all the supllier name, who are supplying 'Pencil 3B' 
val results1 = sqlContext.sql( "SELECT p.name AS productName, s.name AS supplierName FROM products_suppliers AS ps JOIN product AS p ON ps.productID = p.productID JOIN supplier AS s ON ps.supplierID = s.supplierID WHERE p.name = 'pencil 3B'" ) 
results1.show() 
+-----------+------------+
|productName|supplierName|
+-----------+------------+
|  pencil 3B| XYZ company|
|  pencil 3B|     QQ corp|
+-----------+------------+


Step 3 : Find all the products , which are supplied by ABC Traders. 
val results2 = sqlContext.sql("SELECT p.name AS productName, s.name AS supplierName FROM product AS p, products_suppliers AS ps, supplier AS s WHERE p.productID = ps.productID AND ps.supplierID = s.supplierID AND s.name = 'ABC Traders'") 

results2.show() 


Step 4 : Find all the products , which are supplied by XYZ company. 
val results3 = sqlContext.sql("SELECT p.name AS product_Name, s.name AS supplierName FROM product AS p, products_suppliers AS ps, supplier AS s WHERE p.productID = ps.productID AND ps.supplierID = s.supplierID AND s.name = 'XYZ company'") 

results3.show() 
+-----------+------------+
|productName|supplierName|
+-----------+------------+
|  pencil 3B| XYZ company|
|  pencil 4B| XYZ company|
|  pencil 5B| XYZ company|
+-----------+------------+
