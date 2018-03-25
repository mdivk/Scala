sequenceFile

import org.apache.hadoop.io._
val products = sc.textFile("/public/retail_db/products")
products.map(rec => (NullWritable.get(), rec)).
  saveAsSequenceFile("/user/paslechoix/products_seq")

//Reading sequnce files
sc.sequenceFile("/user/paslechoix/products_seq", classOf[NullWritable], classOf[Text]).
  map(rec => rec._2.toString()).
  collect().
  foreach(println)


parquet files

write: sc.textFile("products").toDF.write.parquet("products_par")

read: sqlContext.read.parquet("products_par").show

val r = sqlContext.read.parquet("products_par").rdd

avro files

write: 
import com.databricks.spark.avro._
sc.textFile("products").toDF.write.avro("products_avro")

read:
sqlContext.read.avro("products_avro")

json files

write:
sc.textFile("products").toDF.toJSON.saveAsTextFile("products_json0325")

read: 
sqlContext.read.json("products_json0325")
