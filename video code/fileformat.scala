Reading and writing into various file formats in hdfs. 

RAW data:
orders table in mysql

Sqoop import:
TextFile (default format):

sqoop import \
--connect=jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=orders \
--target-dir=orders03131



[paslechoix@gw03 ~]$ hdfs dfs -ls orders03131
Found 5 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-13 12:04 orders03131/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     741614 2018-03-13 12:04 orders03131/part-m-00000
-rw-r--r--   3 paslechoix hdfs     753022 2018-03-13 12:04 orders03131/part-m-00001
-rw-r--r--   3 paslechoix hdfs     752368 2018-03-13 12:04 orders03131/part-m-00002
-rw-r--r--   3 paslechoix hdfs     752940 2018-03-13 12:04 orders03131/part-m-00003

sqoop import \
--connect=jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=orders \
--target-dir=orders03132_seq \
--as-sequencefile

[paslechoix@gw03 ~]$ hdfs dfs -ls orders03132_seq
Found 5 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-13 16:54 orders03132_seq/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     880159 2018-03-13 16:54 orders03132_seq/part-m-00000
-rw-r--r--   3 paslechoix hdfs     880420 2018-03-13 16:54 orders03132_seq/part-m-00001
-rw-r--r--   3 paslechoix hdfs     879621 2018-03-13 16:54 orders03132_seq/part-m-00002
-rw-r--r--   3 paslechoix hdfs     880255 2018-03-13 16:54 orders03132_seq/part-m-00003


[paslechoix@gw03 ~]$ hdfs dfs -cat orders03132_seq/part-m-00000 | head
SEQ!org.apache.hadoop.io.LongWritableordeG�Y���&���]E�@��-OCLOSED@��PENDING_PAYMENT@��/COMPLETE@��"{CLOSED@��,COMPLETE@�COMPLETE@��COMPLET@��
                                                                                                                                                _
PROCESSING0     @��PENDING_PAYMENT0
@��PENDING_PAYMENT/
sc.sequenceFile("orders03132_seq/part-m-00000", classOf[Int], classOf[String]).first
sc.hadoopFile[K, V, SequenceFileInputFormat[K,V]]("orders03132_seq/part-m-00000")

With Compress option

sqoop import \
--connect=jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=orders \
--target-dir=orders03131_compressed \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec


[paslechoix@gw03 ~]$ hdfs dfs -ls orders03131_compressed
Found 5 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-13 12:27 orders03131_compressed/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     215683 2018-03-13 12:27 orders03131_compressed/part-m-00000.snappy
-rw-r--r--   3 paslechoix hdfs     215734 2018-03-13 12:27 orders03131_compressed/part-m-00001.snappy
-rw-r--r--   3 paslechoix hdfs     217472 2018-03-13 12:27 orders03131_compressed/part-m-00002.snappy
-rw-r--r--   3 paslechoix hdfs     229833 2018-03-13 12:27 orders03131_compressed/part-m-00003.snappy



File Format	Action	Procedure and points to remember

TEXT FILE	
READ	
sparkContext.textFile(<path to file>);

sample data:
orders03121

sc.textFile("orders03121").first
res0: String = 1,CLOSED


WRITE	
sparkContext.saveAsTextFile(<path to file>,classOf[compressionCodecClass]);

scala> sc.saveAsTextFile("orders03121GzipCodec", classOf[GzipCodec])
<console>:28: error: value saveAsTextFile is not a member of org.apache.spark.SparkContext
              sc.saveAsTextFile("orders03121GzipCodec", classOf[GzipCodec])
//use any codec here org.apache.hadoop.io.compress.(BZip2Codec or GZipCodec or SnappyCodec)


SEQUENCE FILE	
READ	
sparkContext.sequenceFile(<path location>,classOf[<class name>],classOf[<compressionCodecClass >]);
//read the head of sequence file to understand what two class names need to be used here



WRITE	
rdd.saveAsSequenceFile(<path location>, Some(classOf[compressionCodecClass]))
//use any codec here (BZip2Codec,GZipCodec,SnappyCodec)

//here rdd is MapPartitionRDD and not the regular pair RDD.

PARQUET FILE


sqoop import \
--connect=jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=orders \
--target-dir=orders0314_par \
--as-parquetfile	


[paslechoix@gw03 ~]$ hdfs dfs -ls orders0314_par
Found 5 items
drwxr-xr-x   - paslechoix hdfs          0 2018-03-14 16:28 orders0314_par/.metadata
-rw-r--r--   3 paslechoix hdfs     147301 2018-03-14 16:29 orders0314_par/2f2925cb-3af6-42b7-901e-155e98beade1.parquet
-rw-r--r--   3 paslechoix hdfs     147127 2018-03-14 16:29 orders0314_par/b941b39c-00d0-4194-9cde-41262302dad4.parquet
-rw-r--r--   3 paslechoix hdfs     147085 2018-03-14 16:29 orders0314_par/bde74e4c-fee2-4365-ac37-0f0ae356dab9.parquet
-rw-r--r--   3 paslechoix hdfs     151562 2018-03-14 16:29 orders0314_par/e37fd729-f942-4174-bc6f-f92f0d8f298b.parquet


READ	//use data frame to load the file.
sqlContext.read.parquet("orders0314_par").show
+--------+-------------+-----------------+---------------+
|order_id|   order_date|order_customer_id|   order_status|
+--------+-------------+-----------------+---------------+
|   34443|1393045200000|             2742|         CLOSED|
|   34444|1393045200000|             9487|       COMPLETE|
|   34445|1393045200000|             2391|        ON_HOLD|
|   34446|1393045200000|             9912|         CLOSED|
|   34447|1393045200000|             2826|     PROCESSING|
|   34448|1393045200000|             7155|       CANCELED|
|   34449|1393045200000|             5724|     PROCESSING|
|   34450|1393045200000|             7992|       COMPLETE|
|   34451|1393045200000|            11255|       COMPLETE|
|   34452|1393045200000|             8512|        PENDING|
|   34453|1393045200000|             9582|       COMPLETE|
|   34454|1393045200000|             2020|       CANCELED|
|   34455|1393045200000|             4983|PENDING_PAYMENT|
|   34456|1393045200000|             8538|       COMPLETE|
|   34457|1393045200000|             3164|PENDING_PAYMENT|
|   34458|1393045200000|             7964|        ON_HOLD|
|   34459|1393045200000|             3995|       COMPLETE|
|   34460|1393045200000|             2653|       COMPLETE|
|   34461|1393045200000|             2220|SUSPECTED_FRAUD|
|   34462|1393045200000|              392|       COMPLETE|
+--------+-------------+-----------------+---------------+
only showing top 20 rows

Save back to another location:

sqlContext.read.parquet("orders0314_par").write.parquet("orders0314_par_writeback")

[paslechoix@gw03 ~]$ hdfs dfs -ls orders0314_par_writeback
Found 7 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-14 16:54 orders0314_par_writeback/_SUCCESS
-rw-r--r--   3 paslechoix hdfs        497 2018-03-14 16:54 orders0314_par_writeback/_common_metadata
-rw-r--r--   3 paslechoix hdfs       2603 2018-03-14 16:54 orders0314_par_writeback/_metadata
-rw-r--r--   3 paslechoix hdfs      85098 2018-03-14 16:54 orders0314_par_writeback/part-r-00000-5880d6f6-361d-4f93-824f-f337c43bcd5b.gz.parquet
-rw-r--r--   3 paslechoix hdfs      85076 2018-03-14 16:54 orders0314_par_writeback/part-r-00001-5880d6f6-361d-4f93-824f-f337c43bcd5b.gz.parquet
-rw-r--r--   3 paslechoix hdfs      84964 2018-03-14 16:54 orders0314_par_writeback/part-r-00002-5880d6f6-361d-4f93-824f-f337c43bcd5b.gz.parquet
-rw-r--r--   3 paslechoix hdfs      88275 2018-03-14 16:54 orders0314_par_writeback/part-r-00003-5880d6f6-361d-4f93-824f-f337c43bcd5b.gz.parquet


ORC FILE	

READ	

sqlContext.read.orc(<path to location>); //this results in a dataframe

WRITE	

df.write.mode(SaveMode.Overwrite).format("orc") .save(<path to location>)

AVRO FILE	

sqoop import \
--connect=jdbc:mysql://ms.itversity.com/retail_db \
--username=retail_user \
--password=itversity \
--table=orders \
--target-dir=orders0314_avro \
--as-avrodatafile


[paslechoix@gw03 ~]$ hdfs dfs -ls orders0314_avro
Found 5 items
-rw-r--r--   3 paslechoix hdfs          0 2018-03-14 16:58 orders0314_avro/_SUCCESS
-rw-r--r--   3 paslechoix hdfs     439146 2018-03-14 16:58 orders0314_avro/part-m-00000.avro
-rw-r--r--   3 paslechoix hdfs     447726 2018-03-14 16:58 orders0314_avro/part-m-00001.avro
-rw-r--r--   3 paslechoix hdfs     446959 2018-03-14 16:58 orders0314_avro/part-m-00002.avro
-rw-r--r--   3 paslechoix hdfs     447606 2018-03-14 16:58 orders0314_avro/part-m-00003.avro


READ	
import com.databricks.spark.avro._
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.avro").load("input dir")
df.filter("age > 5").write.format("com.databricks.spark.avro").save("output dir")

import com.databricks.spark.avro._
sqlContext.read.avro("orders0314_avro">)

WRITE	
sqlContext.setConf("spark.sql.avro.compression.codec","snappy") //use snappy, deflate, uncompressed;
dataFrame.write.avro(<path to location>);

JSON FILE	
READ	
sqlContext.read.json();

WRITE	
dataFrame.toJSON().saveAsTextFile(<path to location>,classOf[Compression Codec])


sc.hadoopFile[K, V, SequenceFileInputFormat[K,V]]("path/to/file")