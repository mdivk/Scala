Problem Scenario 83 : In Continuation of previous question, please accomplish following activities. 
1. Select all the records with quantity >= 5000 and name starts with 'Pen' 
2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen' 
3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen' 
4. Select all the products which name is 'Pen Red', 'Pen Black' 
5. select all the products WhiCh has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000. 

Solution : 

scala> val prdRDD = products.map(p=>(p.split(",")(0).toInt, p.split(",")(1), p.split(",")(2), p.split(",")(3), p.split(",")(4).toFloat))
scala> val prdDF = prdRDD.toDF("productlD","productCode","name","quantity","price")
prdDF.registerTempTable("products")

val result = sqlContext.sql("select * from products")
result.show


Step 1 : Select all the records with quantity >= 5000 and name starts with 'Per' 

val result1 = sqlContext.sql("select * from products where name like 'Per%'")
result1.show
+---------+-----------+--------------------+--------+------+
|productlD|productCode|                name|quantity| price|
+---------+-----------+--------------------+--------+------+
|      362|         17|Perfect Fitness P...|        | 29.99|
|      365|         17|Perfect Fitness P...|        | 59.99|
|      372|         17|Perfect Ab Carver...|        | 39.99|
|      373|         17|Perfect Fitness M...|        | 39.99|
|      374|         17|Perfect Pushup BASIC|        | 19.99|
|      376|         17|Perfect Pushup V2...|        | 29.99|
|      377|         17|Perfect Pullup Basic|        | 19.99|
|      379|         17|Perfect Multi-Gym...|        | 29.99|
|     1013|         46|Perception Sport ...|        |349.99|
|     1030|         46|Perception Sport ...|        |499.99|
|     1065|         48|Perception Sport ...|        |349.99|
|     1093|         49|Perception Sport ...|        |349.99|
+---------+-----------+--------------------+--------+------+


Step 2 : Select all the records with quantity >=5000 , price is less than 100 and name starts with 'Per' 

val result2 = sqlContext.sql( "SELECT * FROM products WHERE price < 100")
result2.show() 

Note: the data source has some issue and needs to be sanitized before it stops throwing error.

Troublehooting:
1. find out the source data on hdfs, load it into a new hive table
2. check the hive table to find out if there are extra malformed records like null in any columns

To create hive table:
hdfs dfs -cp /public/retail_db/products/part-00000 .

create external table products_hive (product_id int, product_category_id int, product_name string, product_description string, product_price decimal(8,2), product_image string)
row format delimited fields terminated by ','
location '/user/paslechoix/products/';

1345
Time taken: 50.443 seconds, Fetched: 1 row(s)
hive (paslechoix)>
