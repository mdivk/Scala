For the question, the original table contains fields as below:

product_id int(11) NOT NULL AUTO_INCREMENT, 
product_category_id int(11) NOT NULL,
product_name varchar(45) NOT NULL,  
product_description varchar(255) NOT NULL, 
product_price float NOT NULL,
product_image varchar(255) NOT NULL

question: to get the top 3 priced products in each category.



case class Info(product_id: Int, product_category_id: Int, product_name: String, product_price: Double)

val products = sc.textFile("products").map(line => line.split(",")(4)!="")
val productsf = sc.textFile("products").filter(line => line.split(",")(4)!="")

val infos = productsf.map { split =>
    Info(
        product_category_id = split(1).toInt,
        product_name = split(2),
        product_price = split(4).toFloat
    )
}

scala> val infos = productsf.map { split =>
     |     Info(
     |         product_category_id = split(1).toInt,
     |         product_name = split(2),
     |         product_price = split(4).toFloat
     |     )
     | }
<console>:30: error: not found: value Info
           Info(
           ^

val prd = productsf.map(rec => (rec.split(","))).map(line=>(line(0).toInt, line(1).toInt, line(2), line(4).toFloat))

val sorted = prd.sortBy(rec => (rec._1, -rec._2))
import org.apache.spark.mllib.rdd.MLPairRDDFunctions
val keyByCategory = prd.keyBy(_._1)

as the class Info was not created successfully earlier, the following command is expected to fail:
val topByKey: RDD[(Int, Array[Info])] = keyByCategory.topByKey(3)

changed to:
val topByKey = keyByCategory.topByKey(3)

scala> val topByKey = keyByCategory.topByKey(3)
<console>:34: error: value topByKey is not a member of org.apache.spark.rdd.RDD[(Int, (Int, Int, String, Float))]
         val topByKey = keyByCategory.topByKey(3)


Since I already imported org.apache.spark.mllib.rdd.MLPairRDDFunctions, why it throws out error?

Suspected issue here:

scala> keyByCategory
res17: org.apache.spark.rdd.RDD[(Int, (Int, Int, String, Float))] = MapPartitionsRDD[30] at keyBy at :34

The official doc says topByKey is availble to RDD: RDD<scala.Tuple2<K,Object>>, does the error implicating the issue relies on the keyByCategory is not an RDD?

It would be greatly appreciated if someone can enlighten me here? is RDD[(Int, (Int, Int, String, Float))] not RDD<scala.Tuple2<K,Object>>?

Thank you very much.