val p = sc.textFile("products")
p.count = 1345
val fp = p.filter(r=>r.split(",")(4) !="")
fp.count = 1344
fp.map(r=>(r.split(",")(4).toFloat, r)).sortByKey(false).take(10).foreach(println)
/*(1999.99,208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical)
(1799.99,66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1099.99,1048,47,Spalding Beast 60" Glass Portable Basketball ,,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop)
(999.99,60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)(999.99,695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)*/

fp.map(r=>(r.split(",")(4).toFloat, r)).sortByKey(false).map(r=> (r._2.split(",")(4), r._2.split(",")(1), r._2.split(",")(2), r._2.split(",")(3))).take(5).foreach(println)


val mfp = fp.map(r=>(r.split(",")(4).toFloat, r)).sortByKey(false).map(r=> (r._2.split(",")(4), r._2.split(",")(2)))
res2: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[7] at map at <console>:31
res1: (String, String) = (1999.99,SOLE E35 Elliptical)

mfp.sortBy(r=>(r._1, r._2)).map(_2, _1).take(10).foreach(println)

val first = fp.map(r=>(r.split(",")(4).toFloat, r))

first._2.split(",")(4)





1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy

val fpmap = fp.map(r=>(r.split(",")(1).toInt, r.split(",")(3), r.split(",")(4).toFloat))
val gfpmap = fpmap.map(t => (t._1, t)).groupByKey()
fpmap.flatMap { case (k, tuples) => tuples.toList.sortBy(-_._3).take(3) }
  .sortBy(_._1)
  .collect


scala> val a = sc.parallelize(List((3, 0, 5), (2, 0, 4), (3, 0, 4), (3, 0, 12), (2, 0, 7), (3, 5, 6)))
(3,0,5)
(2,0,4)
(3,0,4)
(3,0,12)
(2,0,7)
(3,5,6)

scala> val b = a.map(t=>(t._1, t)).groupByKey
(2,CompactBuffer((2,0,4), (2,0,7)))
(3,CompactBuffer((3,0,5), (3,0,4), (3,0,12), (3,5,6)))

scala> val c = b.flatMap{case (k,tuples)=>tuples.toList.sortBy(-_._3).take(3)}
(2,0,7)
(2,0,4)
(3,0,12)
(3,5,6)
(3,0,5)

Now the result is sorted by ._3 in desc order

scala> c.sortBy(_._1).take(10).foreach(println)
(2,0,7)
(2,0,4)
(3,0,12)
(3,5,6)
(3,0,5)

Based on the previous result, sort it again on _1 in asc order 





scala> val a = sc.textFile("products")
scala> val af = a.filter(r=>r.split(",")(4) !="")
scala> val b = af.map(t=>(t.split(",")(1).toInt, t.split(",")(2), t.split(",")(4).toFloat)).groupByKey
res43: (Int, String, Float) = (2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
scala> val bm = b.map(r=>(r._1, (r._2, r._3)))
res45: (Int, (String, Float)) = (2,(Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98))
scala> val bmg = bm.groupByKey
res47: (Int, Iterable[(String, Float)]) = 
	(34,
		CompactBuffer((FootJoy GreenJoys Golf Shoes,59.99), 
						(FootJoy GreenJoys Golf Shoes,59.99), 
						(Ogio Race Golf Shoes,169.99), 
						(Ogio Race Golf Shoes,169.99), 
						(Ogio City Spiked Golf Shoes,149.99), 
						(Ogio City Spiked Golf Shoes,149.99), 
						(Ogio City Spiked Golf Shoes,149.99), 
						(Ogio City Turf Golf Shoes,129.99), 
						(Ogio City Turf Golf Shoes,129.99), 
						(Ogio City Turf Golf Shoes,129.99), 
						(Ogio Sport Golf Shoes,99.99), 
						(Ogio Sport Golf Shoes,99.99), 
						(Ogio Sport Golf Shoes,99.99), 
						(TRUE linkswear Lyt Dry Golf Shoes,149.99), 
						(TRUE linkswear Lyt Dry Golf Shoes,149.99), 
						(TRUE linkswear Lyt Dry Golf Shoes,149.99), 
						(TRUE linkswear Vegas Golf Shoes,99.99), 
						(TRUE linkswear Vegas Golf Shoes,99.99), 
						(Nike Lunarwaverly Golf Shoes,139.99), 
						(Nike Lunarwaverly Golf Shoes,1...

scala> val c = 




SparkSQL solution


val prd = sc.textFile("products")
val prd_filtered = prd.filter(r=>r.split(",")(4) !="")

val prdMap = prd_filtered.map(r=>r.split(",")).map(rec=>(rec(1), rec(3), rec(4)))
val prdDF = prdMap.toDF("category_id","product_name","product_price")
+-----------+------------+-------------+
|category_id|product_name|product_price|
+-----------+------------+-------------+
|          2|            |        59.98|
|          2|            |       129.99|
|          2|            |        89.99|
|          2|            |        89.99|
|          2|            |       199.99|
+-----------+------------+-------------+
only showing top 5 rows

val query = "
select * from (select * from product group by category_id order by product_price desc limit 3)
"


val query1 = "SELECT *, ROW_NUMBER() as rn OVER (PARTITION BY category_id ORDER BY product_price DESC) FROM product"
val result1 = sqlContext.sql(query1)

select p1.product_id, p1.product_category_id, p1.product_name, p1.product_price from products p1
cross join 
  (select p2.product_id, p2.product_category_id, p2.product_name, p2.product_price from products p2 
  order by p2.product_price desc limit 3) as p3


case class Info(product_category_id: Int, product_name: String, product_price: Double)

val rdd = sc.textFile("products").map(line => line.split(",")).filterNot(_.isEmpty)

val infos = rdd.map { split =>
    Info(
        product_category_id = split(1),
        product_name = split(2),
        product_price = split(4)
    )
}

val sorted = infos.sortBy(info => (info.product_category_id, -info.product_price))

import org.apache.spark.mllib.rdd.MLPairRDDFunctions
val keyByCategory = infos.keyBy(_.product_category_id)
val topByKey: RDD[(Int, Array[Info])] = keyByCategory.topByKey(3)(Ordering.by(-_.product_price))
val topWithKeysSorted = topByKey.sortBy(_._1)



product_id int(11) NOT NULL AUTO_INCREMENT, 
product_category_id int(11) NOT NULL,
product_name varchar(45) NOT NULL,  
product_description varchar(255) NOT NULL, 
product_price float NOT NULL,
product_image varchar(255) NOT NULL




case class Info(product_id: Int, product_category_id: Int, product_name: String, product_price: Double)

val products = sc.textFile("products").map(line => line.split(",")(4)!="")
val productsf = sc.textFile("products").filter(line => line.split(",")(4)!="")

val infos = productsf.map { split =>
    Info(
    	product_id = split(0).toInt,
        product_category_id = split(1).toInt,
        product_name = split(2),
        product_price = split(4).toFloat
    )
}

val prd = productsf.map(rec => (rec.split(","))).map(line=>(line(0).toInt, line(1).toInt, line(2), line(4).toFloat))

val sorted = prd.sortBy(rec => (rec._1, -rec._2))
import org.apache.spark.mllib.rdd.MLPairRDDFunctions
val keyByCategory = prd.keyBy(_._1)
val topByKey: RDD[(Int, Array[Info])] = keyByCategory.topByKey(3)
//val topByKey: RDD[(Int, Array[Info])] = keyByCategory.topByKey(3)(Ordering.by(-_.4))
val topWithKeysSorted = topByKey.sortBy(_._1)


keyByCategory.sortBy()

val products = sc.textFile("products")
val fproducts = products.filter(r=>r.split(",")(4) !="")
val mfproducts = fproducts.map(r=>(r.split(",")(4).toFloat, r)).sortByKey(false).map(r=> (r._2.split(",")(0), r._2.split(",")(1), r._2.split(",")(2), r._2.split(",")(4))
res0: (String, String, String, String) = (208,10,SOLE E35 Elliptical,1999.99)

