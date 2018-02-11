Problem Scenario 78 : You have been given MySQL DB with following details. 
User=retail_dba 
password=cloudera 
Database=retail_db 
table=retail_db.orders 
table=retail_db.order_items 
jdbc URL = jdbc:mysql://quickstart:3306/retail_db 
Columns of order table : (order_id , order_date , order_customer_id, order_status) 
Columns of order_items table : (order_item_id , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
Please accomplish following activities. 
1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory p92_orders and p92_order_items . 
2. Join these data using order_id in Spark and Python 
3. Calculate total revenue perday and per customer 
4. Calculate maximum revenue customer 
======================================================================= 

Solution : 

Step 1 : Import Single table . 
sqoop import –connect jdbc:mysql://quickstart:3306/retail_db –username=retail_dba --password=cloudera -table=orders –target-dir=p92_orders –m1 
sqoop import --connect  jdbc:mysql://quickstart:3306/retail_db --username=retail_dba --password=cloudera -table=order_items --target-dir=p92_order_items –m1
Note:Please check you don’t have space between before or after ‘=’sign.
Sqoop uses the MapReduce framework to copy data from RDBMS to hdfs
Step2: Read the data from one of the partition,created using above command.
hadoop fs -cat p92_orders/part-m-00000
Hadoop fs -cat p92_order_items/part-m-00000
Step3:Load these above two directory as RDD using Spark and Python(Open pyspark terminal and do following).
Orders = sc.textFile(“p92_orders”)
Orderitems = sc.textFile(“p92_order_items”)
Step4: Convert RDD into key value as(oder_id as a key and rest of the values as a value)
#First value is order_id
orderitemsKeyValue = orders.map(lambda line: (int(line.split(“,”)[1]),line))
#second value as an Order_id
orderitemsKeyValue=orderitems.map(lambda line: (int(line.split(“,”)[1]),line))
Step 5:join both the RDD using order_id
joinedData = orderItemsKeyValue.join(ordersKeyValue)
#print the joined data
For line in joinedData.collect():
Print(line)
#Format of joined data
#[Orderid, ‘All columns from orderitemsKeyValue’, ‘All columns from ordersKeyValue’]
ordersPerDatePerCustomer = joinedData.map(lambda line: ((line[1][1].split(“,”)[1],line[1][1].split(“,”)[2]),float(line[1][0].split(“,”)[4])))
amount collectedperdaypercustomer = ordersperdatepercustomer.reducedbykey(lambda runningsum, amount : runningsum + amount)
#(outrecord format will be ((dat,customer_id), totalamount))
For line in amountcollectedperdaypercustomer.collect():
Print(line)
#now change the format of record as(date,(customer_id,total_amount))
revenueperdatepercustomerRDD=amountcollectedperdaypercustomer.map(lambda threelmenttuple: (threeelementuple[0][0], (threeelementtuple[0][1], threeelementtuple[1])))
for line in revenueperdatepercustomerrdd.collect():
print(line)
#calculate maximum amount collected by a customer for each day
Perdatemaxamountcollectedbycustomer=revenueperdatepercustomerrdd.reducedbykey(lambdarunningamounttuple,newamounttuple: (runningamounttuple[1]>=newamounttuple[1] else newamounttuple))
For line in perdatemaxamountcollectedbycustomer.sortbykey().collect():
Print(line)
