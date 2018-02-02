mysql -u retail_user -h ms.itversity.com -p
cd /usr/hdp/current/sqoop-client/lib


sqoop import --connect jdbc:mysql://ms.itversity.com:3306 \
	--username retail_user \
	--password itversity 
	
sqoop list-databases --connect jdbc:mysql://ms.itversity.com:3306 \
	--username retail_user \
	--password itversity 
	
sqoop list-tables --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity 	
	
sqoop eval \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity 		
	--query "select * from orders limit 10"

sqoop eval \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
	--username retail_user
	--password itversity \
	query "CREATE TABLE dummy ( i INT)"
	

sqoop eval \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
	--username retail_user
	--password itversity \
	query "INSERT INTO dummy  VALUES(1)"
	

sqoop eval \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
	--username retail_user
	--password itversity \
	query "SELECT * FROM dummy"
	
Import data from mysql to Hadoop

sqoop import \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/sqoop_import/retail_db/order_items
	--warehouse-dir /user/paslechoix/sqoop_import/retail_db
	
hadoop fs -ls /user/paslechoix/sqoop_import/retail_db
hadoop fs -ls /user/paslechoix/sqoop_import/retail_db/order_items
hadoop fs -tail /user/paslechoix/sqoop_import/retail_db/order_items/part-m-00000
	
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table order_items \
  --warehouse-dir /user/paslechoix/sqoop_import/retail_db \
  --num-mappers 1 \
  --delete-target-dir

 sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table order_items \
  --warehouse-dir /user/paslechoix/sqoop_import/retail_db \
  --num-mappers 1 \
  --append

 sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --target-dir /user/paslechoix/sqoop_import/retail_db \
  --num-mappers 2 \
  --query "select o.*, sum(oi.order_item_subtotal) order_revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id and \$CONDITIONS group by o.order_id, o.order_date, o.order_customer_id, o.order_status" \
  --split-by order_id
 
Sqoop:

1. table and/or columns is mutually exclusive with query 
2. for query, split-by is mandatory if num-mappers is greater than 1, becuase in that case sqoop needs to split data into multiple mappers
3. query must have a placeholder \$CONDITIONS


val orders = sc.textFile("/public/retail_db/orders/part")
val orders_p = sc.textFile("/public/retail_db/orders/part-00000")
  orders_p.count()
  
hdfs dfs -cat /public/retail_db/orders/part-00000 |wc -l	

sqoop import \
 --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
 --username retail_user \
 --password itversity \
 --table orders \
 --target-dir /paslechoix/sqoop_import/retail_db
 --append
	

Import from mysql to HDFS and save as textFile
	
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table orders \
  --warehouse-dir /user/paslechoix/sqoop_import/retail_db \
  --num-mappers 2 \

[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/sqoop_import/retail_db/orders
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-15 13:23 /user/paslechoix/sqoop_import/retail_db/orders/_SUCCESS
-rw-r--r--   3 paslechoix hdfs    1494591 2018-01-15 13:23 /user/paslechoix/sqoop_import/retail_db/orders/part-m-00000
-rw-r--r--   3 paslechoix hdfs    1505353 2018-01-15 13:23 /user/paslechoix/sqoop_import/retail_db/orders/part-m-00001 

[paslechoix@gw01 ~]$ hdfs dfs -tail /user/paslechoix/sqoop_import/retail_db/orders/part-m-00000

34418,2014-02-21 00:00:00.0,10326,PENDING_PAYMENT
34419,2014-02-21 00:00:00.0,11103,PENDING
34420,2014-02-21 00:00:00.0,5917,PROCESSING
34421,2014-02-21 00:00:00.0,7212,COMPLETE
34422,2014-02-21 00:00:00.0,1437,COMPLETE
34423,2014-02-21 00:00:00.0,1838,COMPLETE
34424,2014-02-21 00:00:00.0,8567,COMPLETE
34425,2014-02-21 00:00:00.0,4831,PENDING_PAYMENT
34426,2014-02-21 00:00:00.0,12024,CLOSED
34427,2014-02-21 00:00:00.0,8174,COMPLETE
34428,2014-02-21 00:00:00.0,6112,PENDING
34429,2014-02-21 00:00:00.0,431,PROCESSING
34430,2014-02-21 00:00:00.0,10215,PENDING_PAYMENT
34431,2014-02-21 00:00:00.0,4419,CLOSED
34432,2014-02-21 00:00:00.0,567,PROCESSING
34433,2014-02-21 00:00:00.0,8472,PENDING
34434,2014-02-21 00:00:00.0,8119,CLOSED
34435,2014-02-21 00:00:00.0,3192,PENDING
34436,2014-02-21 00:00:00.0,1169,CLOSED
34437,2014-02-22 00:00:00.0,8053,COMPLETE
34438,2014-02-22 00:00:00.0,8116,COMPLETE
34439,2014-02-22 00:00:00.0,7857,PROCESSING
34440,2014-02-22 00:00:00.0,5921,CLOSED
34441,2014-02-22 00:00:00.0,5778,CLOSED

  
  
Import from mysql to HDFS and save as SequenceFile
  
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table order_items \
  --warehouse-dir /user/paslechoix/sqoop_import/retail_db \
  --num-mappers 2 \
  --as-sequencefile	
  
[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/sqoop_import/retail_db/order_items
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-15 13:24 /user/paslechoix/sqoop_import/retail_db/order_items/_SUCCESS
-rw-r--r--   3 paslechoix hdfs    3999746 2018-01-15 13:24 /user/paslechoix/sqoop_import/retail_db/order_items/part-m-00000
-rw-r--r--   3 paslechoix hdfs    3999746 2018-01-15 13:24 /user/paslechoix/sqoop_import/retail_db/order_items/part-m-00001  

[paslechoix@gw01 ~]$ hdfs dfs -tail /user/paslechoix/sqoop_import/retail_db/order_items/part-m-00000
B�B�P=P>��1CG�qCG�qP>P?��mB���Bo��P?P@��sB���B�P@PA���C�C�PAPB���C��qC��qPBPC��mC���Bo��PCPD��1CG�qCG�qPDPE���Cy�fBG��PEPF���C��qC��qPFPG���C�C�PGPH���C��qC��qPHPI���C��qC��qPIPJ��BW�HA���PJPK���C�BG��PKPL���CBHPLPM���C��qC��qPMPN���CG��B���PNPO��mBo��Bo��POPP���C�C�PPPQ���Cy�fBG��PQPR���C���B���PRPS���CzBH[paslechoix@gw01 ~]$ 



Import from mysql to HDFS and save as avrofile
  
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table products \
  --warehouse-dir /user/paslechoix/sqoop_import/retail_db \
  --num-mappers 2 \
  --as-avrodatafile

[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/sqoop_import/retail_db/products
Found 3 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-15 13:31 /user/paslechoix/sqoop_import/retail_db/products/_SUCCESS
-rw-r--r--   3 paslechoix hdfs      86608 2018-01-15 13:31 /user/paslechoix/sqoop_import/retail_db/products/part-m-00000.avro
-rw-r--r--   3 paslechoix hdfs      89888 2018-01-15 13:31 /user/paslechoix/sqoop_import/retail_db/products/part-m-00001.avro  

[paslechoix@gw01 ~]$ hdfs dfs -tail /user/paslechoix/sqoop_import/retail_db/products/part-m-00000.avro
s+Moab+Rover+Mid+Waterproof+Hiking+Boot�
<ZMerrell Men's All Out Flash Trail Running Sh���B�http://images.acmesports.sports/Merrell+Men%27s+All+Out+Flash+Trail+Running+Shoe�
<ZMerrell Women's All Out Flash Trail Running ���B�http://images.acmesports.sports/Merrell+Women%27s+All+Out+Flash+Trail+Running+Shoe�
<ZMerrell Women's All Out Flash Trail Running ���B�http://images.acmesports.sports/Merrell+Women%27s+All+Out+Flash+Trail+Running+Shoe�
>ZCleveland Golf My Custom Wedge 588 Forged RTq�3C�http://images.acmesports.sports/Cleveland+Golf+My+Custom+Wedge+588+Forged+RTX+Black+Pearl...�
>ZCleveland Golf Elite My Custom Wedge 588 Forq�QC�http://images.acmesports.sports/Cleveland+Golf+Elite+My+Custom+Wedge+588+Forged+RTX+Black...�
>ZCleveland Golf Collegiate My Custom Wedge 58q�QC�http://images.acmesports.sports/Cleveland+Golf+Collegiate+My+Custom+Wedge+588+RTX+Forged...�
>PING G30 Driv��C^http://images.acmesports.sports/PING+G30+Driver��%e:5���
                                                                            ���Ah�





Import from mysql to HDFS and save as parquetfile
  
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table customers \
  --warehouse-dir /user/paslechoix/sqoop_import/retail_db \
  --num-mappers 2 \
  --as-parquetfile	

[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/sqoop_import/retail_db/customers
Found 3 items
drwxr-xr-x   - paslechoix hdfs          0 2018-01-15 13:40 /user/paslechoix/sqoop_import/retail_db/customers/.metadata
-rw-r--r--   3 paslechoix hdfs     150191 2018-01-15 13:41 /user/paslechoix/sqoop_import/retail_db/customers/0a821e98-cbc2-4a50-bc8e-82a839b6f0ca.parquet
-rw-r--r--   3 paslechoix hdfs     150226 2018-01-15 13:41 /user/paslechoix/sqoop_import/retail_db/customers/eb5da0eb-58c2-4096-b4b5-433cab44ee2f.parquet

[paslechoix@gw01 ~]$ hdfs dfs -tail /user/paslechoix/sqoop_import/retail_db/customers/0a821e98-cbc2-4a50-bc8e-82a839b6f0ca.parquet
:"customer_id","sqlType":"4"},{"name":"customer_fname","type":["null","string"],"default":null,"columnName":"customer_fname","sqlType":"12"},{"name":"customer_lname","type":["null","string"],"default":null,"columnName":"customer_lname","sqlType":"12"},{"name":"customer_email","type":["null","string"],"default":null,"columnName":"customer_email","sqlType":"12"},{"name":"customer_password","type":["null","string"],"default":null,"columnName":"customer_password","sqlType":"12"},{"name":"customer_street","type":["null","string"],"default":null,"columnName":"customer_street","sqlType":"12"},{"name":"customer_city","type":["null","string"],"default":null,"columnName":"customer_city","sqlType":"12"},{"name":"customer_state","type":["null","string"],"default":null,"columnName":"customer_state","sqlType":"12"},{"name":"customer_zipcode","type":["null","string"],"default":null,"columnName":"customer_zipcode","sqlType":"12"}],"tableName":"customers"}?;parquet-mr (build 27f71a18579ebac6db2b0e9ac758d64288b6dbff)4PAR1[   
	
	
in scala, read the external

text format:
sqlContext.load("/user/paslechoix/sqoop_import/retail_db/orders").show

parquet format:
sqlContext.load("/user/paslechoix/sqoop_import/retail_db/customers", "parquet").show

20 records returned; 12435 in total

sqlContext.load("/user/paslechoix/sqoop_import/retail_db/customers/0a821e98-cbc2-4a50-bc8e-82a839b6f0ca.parquet", "parquet").show

20 records returned; 6218 in total

sqlContext.load("/user/paslechoix/sqoop_import/retail_db/customers/eb5da0eb-58c2-4096-b4b5-433cab44ee2f.parquet", "parquet").show

20 records returned; 6218 in total


ReDo:
hdfs dfs -rm -r /user/paslechoix/sqoop_import/retail_db/customers

sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table customers \
  --warehouse-dir /user/paslechoix/sqoop_import/retail_db \
  --num-mappers 2 \
  --as-parquetfile	


18/01/15 14:34:57 INFO mapreduce.ImportJobBase: Transferred 299.9307 KB in 27.5948 seconds (10.8691 KB/sec)
18/01/15 14:34:57 INFO mapreduce.ImportJobBase: Retrieved 12435 records.
[paslechoix@gw01 ~]$ hdfs dfs -ls /user/paslechoix/sqoop_import/retail_db/customers
Found 3 items
drwxr-xr-x   - paslechoix hdfs          0 2018-01-15 14:34 /user/paslechoix/sqoop_import/retail_db/customers/.metadata
-rw-r--r--   3 paslechoix hdfs     150205 2018-01-15 14:34 /user/paslechoix/sqoop_import/retail_db/customers/6c28b8bf-198b-473a-aaa7-a0a25da6e977.parquet
-rw-r--r--   3 paslechoix hdfs     150226 2018-01-15 14:34 /user/paslechoix/sqoop_import/retail_db/customers/d54a51dd-54b4-4fb2-a2e7-15f8e02ac8d5.parquet


sqlContext.load("/user/paslechoix/sqoop_import/retail_db/customers/6c28b8bf-198b-473a-aaa7-a0a25da6e977.parquet", "parquet").count


sqlContext.load("/user/paslechoix/sqoop_import/retail_db/customers/d54a51dd-54b4-4fb2-a2e7-15f8e02ac8d5.parquet", "parquet").count


/etc/hadoop/conf/core-site.xml

	<property>
      <name>io.compression.codecs</name>
      <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.SnappyCodec</value>
    </property>
	


sqoop import \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/retail_db/order_items \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.SnappyCodec
	

[paslechoix@gw01 conf]$ hdfs dfs -ls /user/paslechoix/retail_db/order_items
Found 5 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     456557 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00000.snappy
-rw-r--r--   3 paslechoix hdfs     459317 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00001.snappy
-rw-r--r--   3 paslechoix hdfs     458768 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00002.snappy
-rw-r--r--   3 paslechoix hdfs     450824 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00003.snappy
	
	
sqoop import \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/retail_db/order_items_def \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.DefaultCodec

[paslechoix@gw01 conf]$ hdfs dfs -ls /user/paslechoix/retail_db/order_items_def
Found 5 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-15 16:48 /user/paslechoix/retail_db/order_items_def/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     257731 2018-01-15 16:48 /user/paslechoix/retail_db/order_items_def/part-m-00000.deflate
-rw-r--r--   3 paslechoix hdfs     258565 2018-01-15 16:48 /user/paslechoix/retail_db/order_items_def/part-m-00001.deflate
-rw-r--r--   3 paslechoix hdfs     259774 2018-01-15 16:48 /user/paslechoix/retail_db/order_items_def/part-m-00002.deflate
-rw-r--r--   3 paslechoix hdfs     254602 2018-01-15 16:48 /user/paslechoix/retail_db/order_items_def/part-m-00003.deflate	




sqoop import \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/retail_db/order_items_gzip \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.GzipCodec \
	
[paslechoix@gw01 conf]$ hdfs dfs -ls /user/paslechoix/retail_db/order_items_gzip
Found 4 items
-rw-r--r--   3 paslechoix hdfs     257743 2018-01-15 16:46 /user/paslechoix/retail_db/order_items_gzip/part-m-00000.gz
-rw-r--r--   3 paslechoix hdfs     258577 2018-01-15 16:46 /user/paslechoix/retail_db/order_items_gzip/part-m-00001.gz
-rw-r--r--   3 paslechoix hdfs     259786 2018-01-15 16:46 /user/paslechoix/retail_db/order_items_gzip/part-m-00002.gz
-rw-r--r--   3 paslechoix hdfs     254614 2018-01-15 16:46 /user/paslechoix/retail_db/order_items_gzip/part-m-00003.gz


sqoop import \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/retail_db/order_items_gzip \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.GzipCodec \
	--append	
	
[paslechoix@gw01 conf]$ hdfs dfs -ls /user/paslechoix/retail_db/order_items_gzip
Found 8 items
-rw-r--r--   3 paslechoix hdfs     257743 2018-01-15 16:46 /user/paslechoix/retail_db/order_items_gzip/part-m-00000.gz
-rw-r--r--   3 paslechoix hdfs     258577 2018-01-15 16:46 /user/paslechoix/retail_db/order_items_gzip/part-m-00001.gz
-rw-r--r--   3 paslechoix hdfs     259786 2018-01-15 16:46 /user/paslechoix/retail_db/order_items_gzip/part-m-00002.gz
-rw-r--r--   3 paslechoix hdfs     254614 2018-01-15 16:46 /user/paslechoix/retail_db/order_items_gzip/part-m-00003.gz
-rw-r--r--   3 paslechoix hdfs     257743 2018-01-15 16:50 /user/paslechoix/retail_db/order_items_gzip/part-m-00004.gz
-rw-r--r--   3 paslechoix hdfs     258577 2018-01-15 16:50 /user/paslechoix/retail_db/order_items_gzip/part-m-00005.gz
-rw-r--r--   3 paslechoix hdfs     259786 2018-01-15 16:50 /user/paslechoix/retail_db/order_items_gzip/part-m-00006.gz
-rw-r--r--   3 paslechoix hdfs     254614 2018-01-15 16:50 /user/paslechoix/retail_db/order_items_gzip/part-m-00007.gz


sqoop import \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/retail_db/order_items \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.GzipCodec \
	--append

[paslechoix@gw01 conf]$ hdfs dfs -ls /user/paslechoix/retail_db/order_items
Found 9 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     456557 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00000.snappy
-rw-r--r--   3 paslechoix hdfs     459317 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00001.snappy
-rw-r--r--   3 paslechoix hdfs     458768 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00002.snappy
-rw-r--r--   3 paslechoix hdfs     450824 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00003.snappy
-rw-r--r--   3 paslechoix hdfs     257743 2018-01-15 16:55 /user/paslechoix/retail_db/order_items/part-m-00004.gz
-rw-r--r--   3 paslechoix hdfs     258577 2018-01-15 16:55 /user/paslechoix/retail_db/order_items/part-m-00005.gz
-rw-r--r--   3 paslechoix hdfs     259786 2018-01-15 16:55 /user/paslechoix/retail_db/order_items/part-m-00006.gz
-rw-r--r--   3 paslechoix hdfs     254614 2018-01-15 16:55 /user/paslechoix/retail_db/order_items/part-m-00007.gz	

sqoop import \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/retail_db/order_items \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.DefaultCodec \
	--append

	

If import the same table with different compression codec to the same folder, it is clearly showing that default codec has the best compression ratio among the three codec, second is gzip, the last one is snappy.

	
[paslechoix@gw01 conf]$ hdfs dfs -ls /user/paslechoix/retail_db/order_items
Found 13 items
-rw-r--r--   3 paslechoix hdfs          0 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     456557 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00000.snappy
-rw-r--r--   3 paslechoix hdfs     459317 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00001.snappy
-rw-r--r--   3 paslechoix hdfs     458768 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00002.snappy
-rw-r--r--   3 paslechoix hdfs     450824 2018-01-15 16:39 /user/paslechoix/retail_db/order_items/part-m-00003.snappy
-rw-r--r--   3 paslechoix hdfs     257743 2018-01-15 16:55 /user/paslechoix/retail_db/order_items/part-m-00004.gz
-rw-r--r--   3 paslechoix hdfs     258577 2018-01-15 16:55 /user/paslechoix/retail_db/order_items/part-m-00005.gz
-rw-r--r--   3 paslechoix hdfs     259786 2018-01-15 16:55 /user/paslechoix/retail_db/order_items/part-m-00006.gz
-rw-r--r--   3 paslechoix hdfs     254614 2018-01-15 16:55 /user/paslechoix/retail_db/order_items/part-m-00007.gz
-rw-r--r--   3 paslechoix hdfs     257731 2018-01-15 16:58 /user/paslechoix/retail_db/order_items/part-m-00008.deflate
-rw-r--r--   3 paslechoix hdfs     258565 2018-01-15 16:58 /user/paslechoix/retail_db/order_items/part-m-00009.deflate
-rw-r--r--   3 paslechoix hdfs     259774 2018-01-15 16:58 /user/paslechoix/retail_db/order_items/part-m-00010.deflate
-rw-r--r--   3 paslechoix hdfs     254602 2018-01-15 16:58 /user/paslechoix/retail_db/order_items/part-m-00011.deflate
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/retail_db/order_items \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.SnappyCodec
	
Orders

sqoop import \
	--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
	--username retail_user \
	--password itversity \
	--table order_items \
	--target-dir /user/paslechoix/retail_db/order_def \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.DefaultCodec \
	--append

[paslechoix@gw01 conf]$ hdfs dfs -ls /user/paslechoix/retail_db/order_def
Found 4 items
-rw-r--r--   3 paslechoix hdfs     257731 2018-01-15 17:48 /user/paslechoix/retail_db/order_def/part-m-00000.deflate
-rw-r--r--   3 paslechoix hdfs     258565 2018-01-15 17:48 /user/paslechoix/retail_db/order_def/part-m-00001.deflate
-rw-r--r--   3 paslechoix hdfs     259774 2018-01-15 17:48 /user/paslechoix/retail_db/order_def/part-m-00002.deflate
-rw-r--r--   3 paslechoix hdfs     254602 2018-01-15 17:48 /user/paslechoix/retail_db/order_def/part-m-00003.deflate

	

	
Spark
Create RDD from hdfs

File location: /user/paslechoix/retail_db/order_def
in total there are 172198 records (from table order in retail_db in mysql)

To create RDD based on the files:
val orders = sc.textFile("/user/paslechoix/retail_db/order_def")
orders.count
172198

Inspect the data:
val orders = sc.textFile("/user/paslechoix/retail_db/order_def")
orders.first
orders.take(10).foreach(println)

1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
5,4,897,2,49.98,24.99
6,4,365,5,299.95,59.99
7,4,502,3,150.0,50.0
8,4,1014,4,199.92,49.98
9,5,957,1,299.98,299.98
10,5,365,5,299.95,59.99

Create RDD from local file system
File location: 
[paslechoix@gw01 conf]$ cat /data/retail_db/orders/part-00000 | wc -l
68883

val orders_local =  scala.io.Source.fromFile("/data/retail_db/orders/part-00000").getLines.foreach(println)


File location:
hdfs dfs -ls /user/paslechoix/retail_db/order_items


Let's find out how many records in order_items
From mysql:
mysql -u retail_user -h ms.itversity.com -p
172198

val orders = sc.textFile("/user/paslechoix/retail_db/order_items")
orders.count
res0: Long = 516594

Note: despite this is to load text file only, it actually loads all formats: 172198 x 3 = 516594


// 21,2013-07-25 00:00:00.0,11599,CLOSED -> 20130725 as Int
val str = orders.first
str.split(",")(1).substring(0, 10).replace("-", "").toInt

val orderDates = orders.map((str: String) => {
  str.split(",")(1).substring(0, 10).replace("-", "").toInt
})



Flume

A simple example from flume official doc site: http://flume.apache.org/FlumeUserGuide.html

# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444

# Describe the sink
a1.sinks.k1.type = logger

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
	
	
	
Kafka + Spark-Streaming
official site: https://spark.apache.org/docs/1.6.3/streaming-kafka-integration.html 


Case study
Crime by month

Solution by RDD

val crimeData = sc.textFile("/public/crime/csv")
val header = crimeData.first
val crimeDataWithoutHeader = crimeData.filter(criminalRecord => criminalRecord != header)

val rec = crimeDataWithoutHeader.first
val distinctDates = crimeDataWithoutHeader.
	map(criminalRecord => criminalRecord.split(",")(2).split( )(0)).
	distinct.
	collect.
	sorted
	
distinctDates.foreach(println)

val t = {
 val r = rec.split(",")
 val d = r(2).split(" ")(0)
 val m = d.split("/")(2) + d.split("/")(0)
 
 ((m.toInt, r(5)),1)
}

val criminalRecordsWithMonthAndType = crimeDataWithoutHeader.
	map(rec => {
		 val r = rec.split(",")
		 val d = r(2).split(" ")(0)
		 val m = d.split("/")(2) + d.split("/")(0)
		 
		 ((m.toInt, r(5)),1)
	})
	
val crimeCountPerMonthPerType = criminalRecordsWithMonthAndType.
	reduceByKey((total, value) => total + value)

val crimeCountPerMonthPerTypeSorted = crimeCountPerMonthPerType.
	map(rec => ((rec._1._1, -rec._2), rec._1._1 + "\t" + rec._2 + "\t" + rec._1._2)).
	sortByKey()
	
val crimeCountPerMonthPerTypeSortedFinal = crimeCountPerMonthPerTypeSorted.
	map(rec => rec._2)
	
Find out the codec full name

go to hadoop core-site.xml and search for codec to get the codec name

save the result in GZip format

crimeCountPerMonthPerTypeSortedFinal.saveAsTextFile("/user/paslechoix/solutions/solution01/crimes_by_type_by_month", classOf[org.apache.hadoop.io.compress.GzipCodec])	

merge and save as one single file

crimeCountPerMonthPerTypeSortedFinal.
	coalesce(1).
	saveAsTextFile("/user/paslechoix/solutions/solution01/crimes_by_type_by_month", classOf[org.apache.hadoop.io.compress.GzipCodec])	

Solution using DataFrame

val crimeData = sc.textFile("/public/crime/csv")
val header = crimeData.first
val crimeDataWithoutHeader = crimeData.filter(criminalRecord => criminalRecord != header)

val crimeDataWithoutHeaderAndTypeDF = crimeDataWithoutHeader.
	map


	

	
	