scala> import org.apache.hadoop.io.{Text, IntWritable}

val data = sc.sequenceFile("products0317_sequence", classOf[IntWritable], classOf[Text])

if a sequence is correctly generated, it should contain in the head right after SEQ:

https://wiki.apache.org/hadoop/SequenceFile

keyClassName - String
valueClassName - String

Unfortunately it seems all the sequence files generated through sqoop end up with only one class, and hence there is no way to read it out 


[paslechoix@gw03 ~]$ hdfs dfs -cat products0317_sequence/part-m-00000 | head
SEQ!org.apache.hadoop.io.LongWritablproducts    ▒▒H&▒▒d''
▒-Quest Q64 10 FT. x 10 FT. Slant Leg Instant UBo▒Uhttp://images.acmesports