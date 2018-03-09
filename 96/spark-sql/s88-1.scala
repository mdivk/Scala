Problem Scenario 88 : You have been given below three Files 

product.csv (Create this File in hdfs) 

productID,productCode,name,quantity,price,supplierid 

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

products_suppliers.csv 

productID,supplierID

2001,501 
2002, 501 
2003, 501 
2004, 502 
2001 ,503 

1. Preparing:
The above three files are created and copied over to hdfs:
[paslechoix@gw03 s88]$ hdfs dfs -ls s88
Found 3 items
-rw-r--r--   3 paslechoix hdfs        326 2018-03-08 20:26 s88/product.csv
-rw-r--r--   3 paslechoix hdfs         45 2018-03-08 20:27 s88/products_suppliers.csv
-rw-r--r--   3 paslechoix hdfs         48 2018-03-08 20:27 s88/supplier.csv

2. Preparing RDD
val product = sc.textFile("s88/product.csv")
val supplier = sc.textFile("s88/supplier.csv")
val products_suppliers = sc.textFile("s88/products_suppliers.csv")

scala> val product = sc.textFile("s88/product.csv")
18/03/09 17:45:54 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 337.0 KB, free 1433.1 KB)
18/03/09 17:45:54 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 28.4 KB, free 1461.5 KB)
18/03/09 17:45:54 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on localhost:36719 (size: 28.4 KB, free: 511.0 MB)
18/03/09 17:45:54 INFO SparkContext: Created broadcast 3 from textFile at <console>:27
product: org.apache.spark.rdd.RDD[String] = s88/product.csv MapPartitionsRDD[17] at textFile at <console>:27

scala> val supplier = sc.textFile("s88/supplier.csv")
18/03/09 17:45:54 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 337.0 KB, free 1798.5 KB)
18/03/09 17:45:54 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 28.4 KB, free 1826.9 KB)
18/03/09 17:45:54 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on localhost:36719 (size: 28.4 KB, free: 511.0 MB)
18/03/09 17:45:54 INFO SparkContext: Created broadcast 4 from textFile at <console>:27
supplier: org.apache.spark.rdd.RDD[String] = s88/supplier.csv MapPartitionsRDD[19] at textFile at <console>:27

scala> val products_suppliers = sc.textFile("s88/products_suppliers.csv")
18/03/09 17:45:59 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 337.0 KB, free 2.1 MB)
18/03/09 17:45:59 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 28.4 KB, free 2.1 MB)
18/03/09 17:45:59 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on localhost:36719 (size: 28.4 KB, free: 511.0 MB)
18/03/09 17:45:59 INFO SparkContext: Created broadcast 5 from textFile at <console>:27
products_suppliers: org.apache.spark.rdd.RDD[String] = s88/products_suppliers.csv MapPartitionsRDD[21] at textFile at <console>:27


3. Mapping
val productM = product.map(x=>x.split(",")).map(x=>(x(0).toInt, x(1), x(2),x(3).toInt, x(4).toFloat,x(5).toInt))
val supplierM = supplier.map(x=>x.split(",")).map(x=>(x(0).toInt, x(1), x(2).toBigInt))
val products_suppliersM = products_suppliers.map(x=>x.split(",")).map(x=>(x(0).toInt, x(1)))

scala> val productM = product.map(x=>x.split(",")).map(x=>(x(0).toInt, x(1), x(2),x(3).toInt, x(4).toFloat,x(5).toInt))
productM: org.apache.spark.rdd.RDD[(Int, String, String, Int, Float, Int)] = MapPartitionsRDD[23] at map at <console>:29

scala> val supplierM = supplier.map(x=>x.split(",")).map(x=>(x(0).toInt, x(1), x(2).toInt))
supplierM: org.apache.spark.rdd.RDD[(Int, String, Int)] = MapPartitionsRDD[25] at map at <console>:29

scala> val products_suppliersM = products_suppliers.map(x=>x.split(",")).map(x=>(x(0).toInt, x(1)))
products_suppliersM: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[27] at map at <console>:29


4. convert to DF with schema
val pDF = productM.toDF("productID","productCode","name","quantity","price","supplierid")
val sDF = supplierM.toDF("supplierid","name","phone")
val psDF = products_suppliersM.toDF("productID","supplierID")

5. Register tables

pDF.registerTempTable("product")
sDF.registerTempTable("supplier")
psDF.registerTempTable("products_suppliers")



Now accomplish all the queries given in solution. 
1. It is possible that, same product can be supplied by multiple supplier. Now find each product, its price according to each supplier. 
2. Find all the supllier name, who are supplying 'Pencil 3B' 
3. Find all the products, which are supplied by ABC Traders. 

Solution : 

Step 1 : It is possible that, same product can be supplied by multiple supplier. Now find each product , its price according to each supplier. 

val results1 = sqlContext.sql("select p.name as Product_Name, p.price, s.name as Supplier_Name from product p inner join supplier s on s.supplierid = p.supplierid")

cannot resolve 'p.supplierid' given input columns: [supplierid, price, code, quantity, id, sid, name, phone, name]; line 1 pos 119

traced back to the supplierM which has a x(2).toInt throwing our error because of the malformed input "88882222 " failed to be converted into Int, solution is to add trim before toInt 

scala> val supplierM = supplier.map(x=>x.split(",")).map(x=>(x(0).toInt, x(1), x(2).trim.toInt))

val results1 = sqlContext.sql("select p.name as Product_Name, p.price, s.name as Supplier_Name from product p inner join supplier s on s.supplierid = p.supplierid")
+------------+-----+-------------+
|Product_Name|price|Supplier_Name|
+------------+-----+-------------+
|     pen Red| 1.23|  XYZ company|
|    pen blue| 1.25|  XYZ company|
|   pen Black| 1.25|  XYZ company|
|   pencil 3B| 0.52|  XYZ company|
|   pencil 4B| 0.62|  XYZ company|
|   pencil 5B| 0.73|  XYZ company|
+------------+-----+-------------+


val results1 = sqlContext.sql(“SELECT product.name AS Product_Name, price, supplier.name AS supplier_Name FROM products_suppliers JOIN product ON products_suppliers.productID = product.productID jOIN supplier ON products_suppliers.supplierID = supplier.supplierID”)

reformatted as below:

val results1 = sqlContext.sql(""SELECT product.name AS Product_Name, price, supplier.name AS supplier_Name 
	FROM products_suppliers JOIN product ON products_suppliers.productID = product.productID  
	jOIN supplier ON products_suppliers.supplierID = supplier.supplierID""")

val results1 = sqlContext.sql("SELECT product.name AS Product_Name, price, supplier.name AS supplier_Name \
	FROM products_suppliers JOIN product ON products_suppliers.productID = product.productID  \
	jOIN supplier ON products_suppliers.supplierID = supplier.supplierID")

results. show() 

+------------+-----+-------------+
|Product_Name|price|supplier_Name|
+------------+-----+-------------+
|   pencil 3B| 0.52|      QQ corp|
|   pencil 3B| 0.52|  XYZ company|
|   pencil 4B| 0.62|  XYZ company|
|   pencil 5B| 0.73|  XYZ company|
+------------+-----+-------------+


Step 2 : Find all the supllier name, who are supplying 'Pencil 3B' 

val results = sqlContext.sql( "SELECT p.name AS Product_Name, s.name AS Supplier_Name FROM products_suppliers ps JOIN product p ON ps.productID = p.productID JOIN supplier  s ON ps.supplierID = s.supplierID WHERE p.name = 'pencil 3B' ") 

val results = sqlContext.sql( """SELECT p.name AS Product_Name, s.name AS Supplier_Name 
	FROM products_suppliers AS ps 
	JOIN products AS p ON ps.productID = p.productID 
	JOIN suppliers AS s ON ps.supplierID = s.supplierID 
	WHERE p.name = 'Pencil 3B' """) 
results.show() 
+------------+-------------+
|Product_Name|Supplier_Name|
+------------+-------------+
|   pencil 3B|      QQ corp|
|   pencil 3B|  XYZ company|
+------------+-------------+



Step 3 : Find all the products , which are supplied by ABC Traders. 

val results = sqlContext.sql( "SELECT p.name AS Product_Name, s.name AS Supplier_Name FROM product AS p, products_suppliers AS ps, supplier AS s WHERE p.productID = ps.productID AND ps.supplierID = s.supplierID AND s.name = 'QQ corp' ") 
+------------+-------------+
|Product_Name|Supplier_Name|
+------------+-------------+
|   pencil 3B|      QQ corp|
+------------+-------------+
