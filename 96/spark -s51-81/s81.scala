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

Step 0: create the file in file system
[paslechoix@gw01 data]$ cat product.csv
1001,PEN,pen Red,5000,1,23
1002,PEN,pen Blue,8000,1,25
1003,PEN,pen Black,8000,1,25
1004,PEC,Pencil 2B,10000,0.48
1005,PEC,Pencil 2H,8000,0.49
1004,PEC,Pencil HB,0,9999.99


Step 1 : Create this file in HDFS under following directory data/product.csv 
[paslechoix@gw01 data]$ hdfs dfs -put product.csv data/product.csv
[paslechoix@gw01 data]$ hdfs dfs -ls data/product.csv
-rw-r--r--   3 paslechoix hdfs        173 2018-02-11 23:00 data/product.csv
[paslechoix@gw01 data]$ hdfs dfs -cat data/product.csv
1001,PEN,pen Red,5000,1,23
1002,PEN,pen Blue,8000,1,25
1003,PEN,pen Black,8000,1,25
1004,PEC,Pencil 2B,10000,0.48
1005,PEC,Pencil 2H,8000,0.49
1004,PEC,Pencil HB,0,9999.99



Step 2 : Now using Spark-shell read the file as RDD 
//load the data into a new ROD 
val products = sc.textFile("data/product.csv") 


//Return the first element in this ROD 
products.first() 
res8: String = 1001,PEN,pen Red,5000,1,23

Step 3 : Now define the schema using a case class 
case class Product(productid: Integer, code: String, name: String, quantity:lnteger , price: Float) 

Step 4 : create an RDD of Product objects 
val prdRDD = products.map(p=>(p.split(",")(0), p.split(",")(1), p.split(",")(2), p.split(",")(3), p.split(",")(4)))


prdRDD.first() 
res9: (String, String, String, String, String) = (1001,PEN,pen Red,5000,1)

prdRDD.count() 
res10: Long = 6

Step 5 : Now create data frame 
val prdDF = prdRDD.toDF("productlD","productCode","name","quantity","price") 
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

Step 6 : Now store data in hive warehouse directory. (However, table will not be created ) 

scala> import org.apache.spark.sql.SaveMode 
scala> prdDF.write.mode(SaveMode.Overwrite).format("orc").saveAsTable("paslechoix.product_orc_table")

Now go to hive
hive (default)> use paslechoix;
OK
Time taken: 0.978 seconds
hive (paslechoix)> show tables;
OK
categories
customers
db
departments
departments_hive01
order_items
product_orc_table
Time taken: 0.083 seconds, Fetched: 7 row(s)
hive (paslechoix)> select * from product_orc_table;
OK
1001    PEN     pen Red 5000    1
1002    PEN     pen Blue        8000    1
1003    PEN     pen Black       8000    1
1004    PEC     Pencil 2B       10000   0.48
1005    PEC     Pencil 2H       8000    0.49
1004    PEC     Pencil HB       0       9999.99
Time taken: 1.529 seconds, Fetched: 6 row(s)

hive (paslechoix)> desc formatted product_orc_table;
OK
# col_name              data_type               comment

productld               string
productcode             string
name                    string
quantity                string
price                   string

# Detailed Table Information
Database:               paslechoix
Owner:                  paslechoix
CreateTime:             Sun Feb 11 23:20:10 EST 2018
LastAccessTime:         UNKNOWN
Protect Mode:           None
Retention:              0
Location:               hdfs://nn01.itversity.com:8020/apps/hive/warehouse/paslechoix.db/product_orc_table
Table Type:             MANAGED_TABLE



//Create a new external table and specify the table's location to be product_orc_table
CREATE EXTERNAL TABLE products (productid int,code string,name string ,quantity int, price float ) 
STORED AS orc 
LOCATION '/apps/hive/warehouse/paslechoix.db/product_orc_table';

//the new table products is created and contains same data as product_orc_table
hive (paslechoix)> show tables;
OK
categories
customers
db
departments
departments_hive01
order_items
product_orc_table
products
Time taken: 0.047 seconds, Fetched: 8 row(s)
hive (paslechoix)> select * from products;
OK
1001    PEN     pen Red 5000    1.0
1002    PEN     pen Blue        8000    1.0
1003    PEN     pen Black       8000    1.0
1004    PEC     Pencil 2B       10000   0.48
1005    PEC     Pencil 2H       8000    0.49
1004    PEC     Pencil HB       0       9999.99
Time taken: 0.193 seconds, Fetched: 6 row(s)
hive (paslechoix)>


Step 8 : Now create a parquet table 

scala> import org.apache.spark.sql.SaveMode 
scala> prdDF.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("paslechoix.product_parquet_table") 


//check hive: new table is created in paslechoix database 
hive (paslechoix)> show tables;
OK
categories
customers
db
departments
departments_hive01
order_items
product_orc_table
product_parquet_table
products
Time taken: 0.056 seconds, Fetched: 9 row(s)

hive (paslechoix)> desc formatted product_parquet_table;
path                    hdfs://nn01.itversity.com:8020/apps/hive/warehouse/paslechoix.db/product_parquet_table



Step 9 : Now create table using the same location 

CREATE EXTERNAL TABLE products_parquet (productid int,code string,name string ,quantity int, price float ) 
STORED AS parquet 
LOCATION '/apps/hive/warehouse/paslechoix.db/product_parquet_table'; 


Step 10 : Check data has been loaded or not. 

hive (paslechoix)> show tables;
OK
categories
customers
db
departments
departments_hive01
order_items
product_orc_table
product_parquet_table
products
products_parquet
Time taken: 0.017 seconds, Fetched: 10 row(s)

//Verify the table content
select * from products_parquet; 
hive (paslechoix)> select * from products_parquet;
OK
t org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.hadoop.ParquetFileReader$Chunk.readAllPages(ParquetFileReader.java:583)
        at org.apache.parquet.hadoop.ParquetFileReader.readNextRowGroup(ParquetFileReader.java:513)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.checkRead(InternalParquetRecordReader.java:130)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:214)
        at org.apache.parquet.hadoop.ParquetRecordReader.nextKeyValue(ParquetRecordReader.java:227)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:120)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:34:37 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.hadoop.ParquetFileReader$Chunk.readAllPages(ParquetFileReader.java:583)
        at org.apache.parquet.hadoop.ParquetFileReader.readNextRowGroup(ParquetFileReader.java:513)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.checkRead(InternalParquetRecordReader.java:130)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:214)
        at org.apache.parquet.hadoop.ParquetRecordReader.nextKeyValue(ParquetRecordReader.java:227)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:120)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:34:37 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: block read in memory in 71 ms. row count = 4
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:372)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.getSplit(ParquetRecordReaderWrapper.java:255)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:97)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:372)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.getSplit(ParquetRecordReaderWrapper.java:255)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:97)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:372)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.getSplit(ParquetRecordReaderWrapper.java:255)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:97)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:372)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.getSplit(ParquetRecordReaderWrapper.java:255)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:97)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:372)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.getSplit(ParquetRecordReaderWrapper.java:255)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:97)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.hadoop.ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetRecordReader.initializeInternalReader(ParquetRecordReader.java:168)
        at org.apache.parquet.hadoop.ParquetRecordReader.initialize(ParquetRecordReader.java:145)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:117)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetRecordReader.initializeInternalReader(ParquetRecordReader.java:168)
        at org.apache.parquet.hadoop.ParquetRecordReader.initialize(ParquetRecordReader.java:145)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:117)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetRecordReader.initializeInternalReader(ParquetRecordReader.java:168)
        at org.apache.parquet.hadoop.ParquetRecordReader.initialize(ParquetRecordReader.java:145)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:117)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetRecordReader.initializeInternalReader(ParquetRecordReader.java:168)
        at org.apache.parquet.hadoop.ParquetRecordReader.initialize(ParquetRecordReader.java:145)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:117)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetMetadata(ParquetMetadataConverter.java:567)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.readParquetMetadata(ParquetMetadataConverter.java:544)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:431)
        at org.apache.parquet.hadoop.ParquetFileReader.readFooter(ParquetFileReader.java:386)
        at org.apache.parquet.hadoop.ParquetRecordReader.initializeInternalReader(ParquetRecordReader.java:168)
        at org.apache.parquet.hadoop.ParquetRecordReader.initialize(ParquetRecordReader.java:145)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:117)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: RecordReader initialized will read a total of 4 records.
Feb 11, 2018 11:36:39 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: at row 0. reading next block
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.hadoop.ParquetFileReader$Chunk.readAllPages(ParquetFileReader.java:583)
        at org.apache.parquet.hadoop.ParquetFileReader.readNextRowGroup(ParquetFileReader.java:513)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.checkRead(InternalParquetRecordReader.java:130)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:214)
        at org.apache.parquet.hadoop.ParquetRecordReader.nextKeyValue(ParquetRecordReader.java:227)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:120)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at org.apache.parquet.format.converter.ParquetMetadataConverter.fromParquetStatistics(ParquetMetadataConverter.java:263)
        at org.apache.parquet.hadoop.ParquetFileReader$Chunk.readAllPages(ParquetFileReader.java:583)
        at org.apache.parquet.hadoop.ParquetFileReader.readNextRowGroup(ParquetFileReader.java:513)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.checkRead(InternalParquetRecordReader.java:130)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:214)
        at org.apache.parquet.hadoop.ParquetRecordReader.nextKeyValue(ParquetRecordReader.java:227)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:120)
        at org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper.<init>(ParquetRecordReaderWrapper.java:83)
        at org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat.getRecordReader(MapredParquetInputFormat.java:71)
        at org.apache.hadoop.hive.ql.exec.FetchOperator$FetchInputFormatSplit.getRecordReader(FetchOperator.java:694)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getRecordReader(FetchOperator.java:332)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.getNextRow(FetchOperator.java:458)
        at org.apache.hadoop.hive.ql.exec.FetchOperator.pushRow(FetchOperator.java:427)
        at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:146)
        at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:1762)
        at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:236)
        at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:168)
        at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:379)
        at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:739)
        at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:684)
        at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:624)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:233)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:148)
Feb 11, 2018 11:36:39 PM WARNING: org.apache.parquet.CorruptStatistics: Ignoring statistics because created_by could not be parsed (see PARQUET-251): parquet-mr version 1.6.0
org.apache.parquet.VersionParser$VersionParseException: Could not parse created_by: parquet-mr version 1.6.0 using format: (.+) version ((.*) )?\(build ?(.*)\)
        at org.apache.parquet.VersionParser.parse(VersionParser.java:112)
        at org.apache.parquet.CorruptStatistics.shouldIgnoreStatistics(CorruptStatistics.java:60)
        at orFailed with exception java.io.IOException:org.apache.hadoop.hive.ql.metadata.HiveException: java.lang.ClassCastException: org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.IntWritable
Time taken: 0.292 seconds
hive (paslechoix)>
