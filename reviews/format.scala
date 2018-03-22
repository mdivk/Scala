import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wc{
	
	def main(args: Array[String]):Unit = {

		val props = ConfigFactory.load()
		val conf = new SparkConf.
			setMaster(props.getConfig(args(2)).getString("executionMode")).
			setAppName("word count")
		val sc = new SparkContext(conf)

		val randomtext = sc.textFile(args(0))
		randomtext.flatMap(rec=>rec.split(" ")).
			map(rec=>(rec, 1)).
			reduceByKey((agg, value) => agg + value).
			map(_.productIterator.mkString("\t")).
			saveAsTextFile(args(1))
	}


}

[paslechoix@gw03 ~]$ wc article.txt -l
151 article.txt

[paslechoix@gw03 ~]$ hdfs dfs -cat data/article.txt

Search

Cassandra Wiki
Login
ArchitectureInternals
FrontPageRecentChangesFindPageHelpContentsArchitectureInternals
Immutable PageInfoAttachments
General
Configuration file is parsed by DatabaseDescriptor (which also has all the default values, if any)

Thrift generates an API interface in Cassandra.java; the implementation is CassandraServer, and CassandraDaemon ties it together (mostly: handling commitlog replay, and setting up the Thrift plumbing)

CassandraServer turns thrift requests into the internal equivalents, then StorageProxy does the actual work, then CassandraServer turns the results back into thrift again

CQL requests are compiled and executed through QueryProcessor. Note that as of 1.2 we still support both the old cql2 dialect and the cql3, in different packages.

StorageService is kind of the internal counterpart to CassandraDaemon. It handles turning raw gossip into the right internal state and dealing with ring changes, i.e., transferring data to new replicas. TokenMetadata tracks which nodes own what arcs of the ring. Starting in 1.2, each node may have multiple Tokens.

AbstractReplicationStrategy controls what nodes get secondary, tertiary, etc. replicas of each key range. Primary replica is always determined by the token ring (in TokenMetadata) but you can do a lot of variation with the others. SimpleStrategy just puts replicas on the next N-1 nodes in the ring. NetworkTopologyStrategy allows the user to define how many replicas to place in each datacenter, and then takes rack locality into account for each DC -- we want to avoid multiple replicas on the same rack, if possible.

MessagingService handles connection pooling and running internal commands on the appropriate stage (basically, a threaded executorservice). Stages are set up in StageManager; currently there are read, write, and stream stages. (Streaming is for when one node copies large sections of its SSTables to another, for bootstrap or relocation on the ring.) The internal commands are defined in StorageService; look for registerVerbHandlers.

Configuration for the node (administrative stuff, such as which directories to store data in, as well as global configuration, such as which global partitioner to use) is held by DatabaseDescriptor. Per-KS, per-CF, and per-Column metadata are all stored as parts of the Schema: KSMetadata, CFMetadata, ColumnDefinition. See also ConfigurationNotes.

Some historical baggage
Some classes have misleading names, notably ColumnFamily (which represents a single row, not a table of data) and, prior to 2.0, Table (which was renamed to Keyspace).

val textFile = sc.textFile("data/article.txt")
val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
counts.saveAsTextFile("data/article.txt.wc")


val s = sc.textFile("data/article.txt")
val r = s.flatMap(_.split(" ")).map(a=>(a,1)).reduceByKey((a,b)=>(a+b))
r.take(10).foreach(println)
(created,1)
(paper,1)
("Fullness",1)
(input,,1)
(greater,1)
(batches,,1)
(ArchitectureGossip,1)
(not).,1)
(HintedHandoff.,1)
(order,3)

val p = r.map(p => (p._2, p._1)).sortByKey(false)
(151,the)
(65,is)
(64,to)
(57,)
(48,a)
(39,of)
(37,and)
(36,are)
(36,in)
(25,for)

val f = p.map(a=>(a._2, a._1))	




