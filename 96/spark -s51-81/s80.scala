Problem Scenario 80 : You have been given MySQL DB with following details. 

User=retail_dba 
password=cloudera 
database=retail_db 
table=retail_db.products 
jdbc URL = jdbc:mysql://quickstart:3306/retail_db 
Columns of products table : (product_id | product_category_ 
id | product_name | product_description | product_price I product_image ) 
Please accomplish following activities. 
1. Copy "retail_db.products" table to hdfs in a directory p93_products 
2. Now sort the products data sorted by product price per category, use product_category_id colunm to group by category 

======================================================================= 
Solution : 
Step 1 : Import Single table . 
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db –username=retai_dba--password=cloudera -table=products --target-dir=p93_products –m1 
Note : Please check you dont have space between before or after ‘=’sign. 
Sqoop uses the MapReduce framework to copy data from RDBMS to hdfs 
Step 2 : Step 2 : Read the data from one of the partition, created using above command. 
hadoop fs -cat p93_products/part-m-00000 
Step 3 : Load this directory as RDD using Spark and Python (Open pyspark terminal and do following). 
productsRDD = sc.textFile("p93_products") 
Step 4 : Filter empty prices, if exists 
#filter out empty prices lines 
nonempty_lines = productsRDD.filter(lambda x:len(x.split(“,”)[4])>0) 
Step 5 : Create data set like (categroyld, (id,name,price) 
mappedRDD = nonempty_lines.map(lambda line: (line.split(",”)[1],(line.split(“,”)[2],float(line.split(“,”)[4]))) 
for line in mappedRDD.collect(): print(line) 
Step 6 : Now groupBy the all records based on categoryld, which a key on mappedRDD it will produce output like (categoryld, iterable of all lines
For akey/categoryId) 
groupByCategroyld = mappedRDD.groupByKey() 
for line in groupByCategroyid.collect(): print(line) 
Step 7 : Now sort the data in each category based on price in ascending order. 
# sorted is a function to sort an iterable, we can also specify, what would be the key on which we want to sort in this case we have price on which it needs to be sorted. 
groupByCategroyid.map(lambda tuple: sorted(tuple[l], key=lambda tupleValue: tupleValue[2])).take(5) 
Step 8 : Now sort the data in each category based on price in descending order. 
# sorted is a function to sort an iterable, we can also specify, what would be the key on which we want to sort in this case we have price on which it needs to be sorted. 
groupByCategroyid.map(lambda tuple: sorted(tuple[l], key=lambda tupleValue: tupleValue[2] , reverse=True)).take(5)
