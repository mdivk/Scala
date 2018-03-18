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
s.flatMap(_.split(" ")).map(a=>(a,1)).reduceByKey((a,b)=>(a+b)).sortWith(_.2, false).take(10).collect
