Scala Examples
The easiest way to work with Avro data files in Spark applications is by using the DataFrame API. The spark-avro library includes avro methods in SQLContext for reading and writing Avro files:

Scala Example with Function
import com.databricks.spark.avro._

val sqlContext = new SQLContext(sc)

// The Avro records are converted to Spark types, filtered, and
// then written back out as Avro records
val df = sqlContext.read.avro("input dir")
df.filter("age > 5").write.avro("output dir")
You can also specify the format "com.databricks.spark.avro":

Scala Example with Format
import com.databricks.spark.avro._

val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.avro").load("input dir")

df.filter("age > 5").write.format("com.databricks.spark.avro").save("output dir")
Writing Deflate Compressed Records
import com.databricks.spark.avro._

val sqlContext = new SQLContext(sc)

// configuration to use deflate compression
sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")
sqlContext.setConf("spark.sql.avro.deflate.level", "5")

val df = sqlContext.read.avro("input dir")

// writes out compressed Avro records
df.write.avro("output dir")
Writing Partitioned Data
import com.databricks.spark.avro._

val sqlContext = new SQLContext(sc)

import sqlContext.implicits._

val df = Seq(
(2012, 8, "Batman", 9.8),
(2012, 8, "Hero", 8.7),
(2012, 7, "Robot", 5.5),
(2011, 7, "Git", 2.0)).toDF("year", "month", "title", "rating")

df.write.partitionBy("year", "month").avro("output dir")
This code outputs a directory structure like this:

-rw-r--r--   3 hdfs supergroup          0 2015-11-03 14:58 /tmp/output/_SUCCESS
drwxr-xr-x   - hdfs supergroup          0 2015-11-03 14:58 /tmp/output/year=2011
drwxr-xr-x   - hdfs supergroup          0 2015-11-03 14:58 /tmp/output/year=2011/month=7
-rw-r--r--   3 hdfs supergroup        229 2015-11-03 14:58 /tmp/output/year=2011/month=7/part-r-00001-9b89f1bd-7cf8-4ba8-910f-7587c0de5a90.avro
drwxr-xr-x   - hdfs supergroup          0 2015-11-03 14:58 /tmp/output/year=2012
drwxr-xr-x   - hdfs supergroup          0 2015-11-03 14:58 /tmp/output/year=2012/month=7
-rw-r--r--   3 hdfs supergroup        231 2015-11-03 14:58 /tmp/output/year=2012/month=7/part-r-00001-9b89f1bd-7cf8-4ba8-910f-7587c0de5a90.avro
drwxr-xr-x   - hdfs supergroup          0 2015-11-03 14:58 /tmp/output/year=2012/month=8
-rw-r--r--   3 hdfs supergroup        246 2015-11-03 14:58 /tmp/output/year=2012/month=8/part-r-00000-9b89f1bd-7cf8-4ba8-910f-7587c0de5a90.avro
Reading Partitioned Data
import com.databricks.spark.avro._

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.avro("input dir")

df.printSchema()
df.filter("year = 2011").collect().foreach(println)
This code automatically detects the partitioned data and joins it all, so it is treated the same as unpartitioned data. This also queries only the directory required, to decrease disk I/O.

root
|-- title: string (nullable = true)
|-- rating: double (nullable = true)
|-- year: integer (nullable = true)
|-- month: integer (nullable = true)

[Git,2.0,2011,7]
Specifying a Record Name
import com.databricks.spark.avro._

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.avro("input dir")

val name = "AvroTest"
val namespace = "com.cloudera.spark"
val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)

df.write.options(parameters).avro("output dir")