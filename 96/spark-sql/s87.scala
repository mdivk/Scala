Problem Scenario 87 : You have been given below three Files 
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

Supplier.csv
Supplierid,name,phone 
501,ABC Traders,88881111
502,XYZ company,8882222

503,QQ corp,88883333 

products_suppliers.csv 

productlD,supplierID 

2001,501 

2002, 501 

2003, 501 

2004, 502 

2001,503 
Now accomplish all the queries given in solution. 

Select product, its price , its supplier name where product price is less than 0.6 using SparkSQL 
Solution • 

Step 1 : 
hdfs dfs -mkdir sparksq12 
hdfs dfs -put product.csv sparksq12/ 
hdfs dfs -put supplier.csv sparksq12/ 
hdfs dfs -put products_suppliers.csv sparksq12/ 
Step 2 : Now in spark shell 
//this is used to implicitly convert an ROD to a DataFrame. 
import sqlContext.implicits. 
//Import Spark SQL data types and Row. 
import org.apache.spark.sql. 

//load the data into a new RDD 
val products = sc.textFile("sparksql2/product.csv") 
val supplier = sc.textFile("sparksq12/supplier.csv") 
val prdsup = sc.textFile("sparksq12/products_suppliers.csv") 
//Return the first element in this RRD 
products.first() 
supplier.first() 
prdsup.first() 
//define the schema using a case class 
case class Product(productid: Integer, code: String, name: String, quantity:lnteger , price: Float , supplierid:lnteger) 
case class Suplier(supplierid: Integer, name: String, phone: String) 
case class PRDSUP(productid: Integer,supplierid: Integer) 
//create an RRD ot Product objects 
val prdRDD = products.map(_.split(",")).map(p => Product(p(O).tolnt,p(1),p(2),p(3).tolnt,p(4).toFloat,p(5).tolnt )) 
val supRDD = supplier.map(_.split(",")).map(p => Suplier(p(O).tolnt,p(1),p(2))) 
val prdsupRDD = prdsup.map(_.split(",")).map(p => PRDSUP(P(0).TOINT,P(1).tolnt))
prdRDD.tirst() 
prdRDD.count() 
supRDD.first() 
supRDD.count() 
prdsupRDD.nrst() 
prdsupRDD.count() 
//change RDD of product objects to a DataFrame 
val prdDF = prdRDD.tODF() 
val supDF = supRDD.tODF() 
val prdsupDF = prdsupRDD.tODF() 
//register the DataFrame as a temp table 
prdDF.registerTempTable("products") 
supDF.registerTempTable("suppliers") 
prdsupDF.registerTempTable("products_suppliers") 
//Select product, its price , its supplier name where product price is less than 0.6 
val results = sqlContext.sql( """ SELECT products.name, price, suppliers.name as sup_name FROM products JOIN suppliers ON products.supplierID = suppliers.supplierID WHERE PRICE <0.6”””)
results.show() 
