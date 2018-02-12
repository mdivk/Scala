Reading and writing into various file formats in hdfs. 

RAW data:



TextFile:



File Format	Action	Procedure and points to remember
TEXT FILE	READ	sparkContext.textFile(<path to file>);
WRITE	sparkContext.saveAsTextFile(<path to file>,classOf[compressionCodecClass]);
//use any codec here org.apache.hadoop.io.compress.(BZip2Codec or GZipCodec or SnappyCodec)
SEQUENCE FILE	READ	sparkContext.sequenceFile(<path location>,classOf[<class name>],classOf[<compressionCodecClass >]);
//read the head of sequence file to understand what two class names need to be used here
WRITE	rdd.saveAsSequenceFile(<path location>, Some(classOf[compressionCodecClass]))
//use any codec here (BZip2Codec,GZipCodec,SnappyCodec)
//here rdd is MapPartitionRDD and not the regular pair RDD.
PARQUET FILE	READ	//use data frame to load the file.
sqlContext.read.parquet(<path to location>); //this results in a data frame object.
WRITE	sqlContext.setConf("spark.sql.parquet.compression.codec","gzip") //use gzip, snappy, lzo or uncompressed here
dataFrame.write.parquet(<path to location>);
ORC FILE	READ	sqlContext.read.orc(<path to location>); //this results in a dataframe
WRITE	df.write.mode(SaveMode.Overwrite).format("orc") .save(<path to location>)
AVRO FILE	READ	import com.databricks.spark.avro._;
sqlContext.read.avro(<path to location>); // this results in a data frame object
WRITE	sqlContext.setConf("spark.sql.avro.compression.codec","snappy") //use snappy, deflate, uncompressed;
dataFrame.write.avro(<path to location>);
JSON FILE	READ	sqlContext.read.json();
WRITE	dataFrame.toJSON().saveAsTextFile(<path to location>,classOf[Compression Codec])