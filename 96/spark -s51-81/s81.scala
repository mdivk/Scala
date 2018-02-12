Problem Scenario 81 : You have been given MySQL DB with following details. 
You have been given following product.csv file 
product.csv (Create this file in hdfs) 
productlD,productCode,name,quantity,price 
1001,PEN,pen Red,5000,1,23
1002,PEN,pen Blue,8000,1,25 
1003,PEN,pen Black,8000,1,25
1004,PEC,Pencil 2B,10000,0.48
1005,PEC,Pencil 2H,8000,0.49
1004,PEC,Pencil HB,0,9999.99
Now accomplish following activities. 
1.Create a Hive ORC table using SparkSql 
2. Load this data in Hive table. 
3. Create a Hive parquet table using SparkSQL and load data in it. 

=================================================================== 

Solution : 

Step 1 : Create this file in HDFS under following directory (Without header)/user/cloudera/he/exam/task1/product.csv 
Step 2 : Now using Spark-shell read the file as RDD 
//load the data into a new ROD 
val products = sc.textFile("/user/cloudera/he/exam/task1/product.csv") 
//Return the first element in this ROD 
products.first() 
Step 3 : Now define the schema using a case class 
case class Product(productid: Integer, code: String, name: String, quantity:lnteger , price: Float) 
Step 4 : create an RDD of Product objects 
val prdRDD = Product.map(_.split(“,”)).map(p =>product(p(0).toint,p(1),p(2),p(3).toint,p(4).toFloat)) 
prdRDD.first() 
prdRDD.count() 
Step 5 : Now create data frame 
val prdDF = prdRDD.tODF() 
Step 6 : Now store data in hive warehouse directory. (However, table will not be created ) 
import org.apache.spark.sql.SaveMode 
prdDF.write.mode(SaveMode.Overwrite).format("orc").saveAsTable("product_orc_table") 
Step 7 : Now create table using data stored in warehouse directory. With the help ot hive. 
hive 
show tables 
CREATE EXTERNAL TABLE products (productid int,code string,name string ,quantity int, price float ) 
STORED AS orc 
LOCATION '/user/hive/warehouse/product_orc_table'; 
Step 8 : Now create a parquet table 
import org.apache.spark.sql.SaveMode 
prdDF.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("product_parquet_table") 
Step 9 : Now create table using this 
CREATE EXTERNAL TABLE products_parquet (productid int,code string,name string ,quantity int, price float ) 
STORED AS parquet 
LOCATION 'luser/hive/warehouse/product_parquet_table'; 
Step 10 : Check data has been loaded or not. 
select * from products; 
select * from products_parquet; 
