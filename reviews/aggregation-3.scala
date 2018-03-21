products:

 products | CREATE TABLE `products` (
  `product_id` int(11) NOT NULL AUTO_INCREMENT,
  `product_category_id` int(11) NOT NULL,
  `product_name` varchar(45) NOT NULL,
  `product_description` varchar(255) NOT NULL,
  `product_price` float NOT NULL,
  `product_image` varchar(255) NOT NULL,
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1346 DEFAULT CHARSET=utf8 

Categories:
mysql> select * from categories limit 5;
+-------------+------------------------+---------------------+
| category_id | category_department_id | category_name       |
+-------------+------------------------+---------------------+
|           1 |                      2 | Football            |
|           2 |                      2 | Soccer              |
|           3 |                      2 | Baseball & Softball |
|           4 |                      2 | Basketball          |
|           5 |                      2 | Lacrosse            |
+-------------+------------------------+---------------------+

RDD solution:


SortByKey

// Ranking - Global (details of top 10 products)
val products = sc.textFile("/public/retail_db/products")

res0: String = 1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy

val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(4).toFloat, product))

res1: (Float, String) = (59.98,1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)
  
// Sorted By Key set to false, meaning it is sorted by value, in this case, key is Float the price and value is the product the String, as it is set to false, meaning sorted by Key, i.e. Float 
val productsSortedByPrice = productsMap.sortByKey(false)
productsSortedByPrice.take(10).foreach(println)
(1999.99,208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical)
(1799.99,66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1099.99,1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop)
(999.99,60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)
(999.99,695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)

val productsSortedByPrice2 = productsMap.sortBy(_._1, false).take(10).foreach(println)
(1999.99,208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical)
(1799.99,66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1799.99,496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill)
(1099.99,1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop)
(999.99,60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical)
(999.99,694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)
(999.99,695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)


val products = sc.textFile("/public/retail_db/products")

products.
  filter(product => product.split(",")(4) != "").
  takeOrdered(10)(Ordering[Float].reverse.on(product => product.split(",")(4).toFloat)).
  foreach(println)
208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical
496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop
60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical
695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...
197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical
694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...
488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical

val productsMap2 = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product, product.split(",")(4).toFloat))
productsMap2.take(10).foreach(println)

productsMap2.sortBy(_._2, false).take(10).foreach(println)  
scala> productsMap2.sortBy(_._2, false).take(10).foreach(println)
(208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical,1999.99)
(66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill,1799.99)
(199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill,1799.99)
(496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill,1799.99)
(1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop,1099.99)
(60,4,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical,999.99)
(197,10,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical,999.99)
(488,22,SOLE E25 Elliptical,,999.99,http://images.acmesports.sports/SOLE+E25+Elliptical,999.99)
(694,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...,999.99)
(695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...,999.99)

Note: as productsMap2 has been mapped to:
map(product => (product, product.split(",")(4).toFloat))
meaning the first element is product, the second element is price 
to sort by price, it should take _._2, there is no _._4



Top N			  
// Ranking - Get top N priced products with in each product category
val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))

scala> productsMap.take(10).foreach(println)
(2,1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)
(2,2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)
(2,3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
(2,4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
(2,5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet)
(2,6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat)
(2,7,2,Schutt Youth Recruit Hybrid Custom Football H,,99.99,http://images.acmesports.sports/Schutt+Youth+Recruit+Hybrid+Custom+Football+Helmet+2014)
(2,8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat)
(2,9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,,50.0,http://images.acmesports.sports/Nike+Adult+Vapor+Jet+3.0+Receiver+Gloves)
(2,10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)
  
Converting (K, V) pairs into (K, Iterable)  
val productsGroupByCategory = productsMap.groupByKey			  
productsGroupByCategory.first
res22: (Int, Iterable[String]) = (34,CompactBuffer(741,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 742,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 743,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 744,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 745,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 746,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 747,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 748,34,Ogio City Turf Golf Shoes,,129.99,htt...

The result shows tuple of category (which is an Int) and all the products in that category

Top N 

val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))
val productsGroupByCategory = productsMap.groupByKey
val productsIterable = productsGroupByCategory.first._2
get <K, V> of <productId, price>
val productPrices = productsIterable.map(p=> (p.split(",")(0), p.split(",")(4).toFloat))
productPrices.take(10).foreach(println)
(741,59.99)
(742,59.99)
(743,169.99)
(744,169.99)
(745,149.99)
(746,149.99)
(747,149.99)
(748,129.99)
(749,129.99)
(750,129.99)

//Get the top/bottom N items 
//initially there are 24 items
scala> productsIterable.map(p => p.split(",")(4).toFloat).size
res15: Int = 24

//convert it to Set will automatically group the same items 
scala> val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
productPrices: scala.collection.immutable.Set[Float] = Set(99.99, 169.99, 149.99, 59.99, 129.99, 34.99, 139.99)
scala> productPrices.size
res16: Int = 7

//Get the top n prices 
scala> val topNPrices = productPrices.toList.sortBy(p => -p).take(5)
topNPrices: List[Float] = List(169.99, 149.99, 139.99, 129.99, 99.99)

//Get the bottom n prices
scala> productPrices.toList.sorted
res18: List[Float] = List(34.99, 59.99, 99.99, 129.99, 139.99, 149.99, 169.99)

scala> productPrices.toList.sorted.take(5)
res19: List[Float] = List(34.99, 59.99, 99.99, 129.99, 139.99)

// Function to get top n priced products using Scala collections API

val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))

productsMap.take(10).foreach(println)  
(2,1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)
(2,2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)
(2,3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
(2,4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
(2,5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet)
(2,6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat)
(2,7,2,Schutt Youth Recruit Hybrid Custom Football H,,99.99,http://images.acmesports.sports/Schutt+Youth+Recruit+Hybrid+Custom+Football+Helmet+2014)
(2,8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat)
(2,9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,,50.0,http://images.acmesports.sports/Nike+Adult+Vapor+Jet+3.0+Receiver+Gloves)
(2,10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)


  
val productsGroupByCategory = productsMap.groupByKey
productsGroupByCategory.take(1).foreach(println)
(34,CompactBuffer(741,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 742,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 743,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 744,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 745,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 746,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 747,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 748,34,Ogio City Turf Golf Shoes,,129.99,http://images.acmesports.sports/Ogio+City+Turf+Golf+Shoes, 749,34,Ogio City Turf Golf Shoes,,129.99,http://images.acmesports.sports/Ogio+City+Turf+Golf+Shoes, 750,34,Ogio City Turf Golf Shoes,,129.99,http://images.acmesports.sports/Ogio+City+Turf+Golf+Shoes, 751,34,Ogio Sport Golf Shoes,,99.99,http://images.acmesports.sports/Ogio+Sport+Golf+Shoes, 752,34,Ogio Sport Golf Shoes,,99.99,http://images.acmesports.sports/Ogio+Sport+Golf+Shoes, 753,34,Ogio Sport Golf Shoes,,99.99,http://images.acmesports.sports/Ogio+Sport+Golf+Shoes, 754,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes, 755,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes, 756,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes, 757,34,TRUE linkswear Vegas Golf Shoes,,99.99,http://images.acmesports.sports/TRUE+linkswear+Vegas+Golf+Shoes, 758,34,TRUE linkswear Vegas Golf Shoes,,99.99,http://images.acmesports.sports/TRUE+linkswear+Vegas+Golf+Shoes, 759,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes, 760,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes, 761,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes, 762,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes, 763,34,PING Golf Shoe Bag,,34.99,http://images.acmesports.sports/PING+Golf+Shoe+Bag, 764,34,Nike Lunar Mount Royal Golf Shoes,,99.99,http://images.acmesports.sports/Nike+Lunar+Mount+Royal+Golf+Shoes))


def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {
  val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
  val topNPrices = productPrices.toList.sortBy(p => -p).take(topN)

  val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
  val minOfTopNPrices = topNPrices.min

  val topNPricedProducts = productsSorted.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)

  topNPricedProducts
}

val productsIterable = productsGroupByCategory.first._2
scala> productsIterable
res24: Iterable[String] = CompactBuffer(741,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 742,34,FootJoy GreenJoys Golf Shoes,,59.99,http://images.acmesports.sports/FootJoy+GreenJoys+Golf+Shoes, 743,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 744,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes, 745,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 746,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 747,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes, 748,34,Ogio City Turf Golf Shoes,,129.99,http://images....


getTopNPricedProducts(productsIterable, 3).foreach(println)
743,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes
744,34,Ogio Race Golf Shoes,,169.99,http://images.acmesports.sports/Ogio+Race+Golf+Shoes
745,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes
746,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes
747,34,Ogio City Spiked Golf Shoes,,149.99,http://images.acmesports.sports/Ogio+City+Spiked+Golf+Shoes
754,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes
755,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes
756,34,TRUE linkswear Lyt Dry Golf Shoes,,149.99,http://images.acmesports.sports/TRUE+linkswear+Lyt+Dry+Golf+Shoes
759,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes
760,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes
761,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes
762,34,Nike Lunarwaverly Golf Shoes,,139.99,http://images.acmesports.sports/Nike+Lunarwaverly+Golf+Shoes



SparkSQL solution:

scala> val products = sc.textFile("/public/retail_db/products")
res0: String = 1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy

scala> val categories = sc.textFile("/public/retail_db/categories")
res1: String = 1,2,Football

val productsDF = products.map(x=>(x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4), x.split(",")(5))).toDF("product_id", "product_category_id","product_name","product_description","product_price","product_image")
productsDF.registerTempTable("products")

val categoriesDF = categories.map(x=>(x.split(",")(0), x.split(",")(1), x.split(",")(2))).toDF("category_id","category_department_id","category_name")
categoriesDF.registerTempTable("categories")


get Top N Priced Products:

val result1 = sqlContext.sql("select * from products order by product_price desc limit 10")
result1.show
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|product_id|product_category_id|        product_name|product_description|product_price|       product_image|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|       197|                 10| SOLE E25 Elliptical|                   |       999.99|http://images.acm...|
|       694|                 32|Callaway Women's ...|                   |       999.99|http://images.acm...|
|       695|                 32|Callaway Women's ...|                   |       999.99|http://images.acm...|
|       488|                 22| SOLE E25 Elliptical|                   |       999.99|http://images.acm...|
|        60|                  4| SOLE E25 Elliptical|                   |       999.99|http://images.acm...|
|       719|                 33|Nike Lunar Cypres...|                   |        99.99|http://images.acm...|
|       758|                 34|TRUE linkswear Ve...|                   |        99.99|http://images.acm...|
|       710|                 32|Top Flite Women's...|                   |        99.99|http://images.acm...|
|       753|                 34|Ogio Sport Golf S...|                   |        99.99|http://images.acm...|
|       757|                 34|TRUE linkswear Ve...|                   |        99.99|http://images.acm...|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+

get Top N priced products with category name 

val result2 = sqlContext.sql("select c.category_name, p.product_name, p.product_price from products p inner join categories c on c.category_id = p.product_category_id group by c.category_name, p.product_name, p.product_price order by c.category_name, p.product_price desc limit 100")
result2.show

+-------------------+--------------------+-------------+
|      category_name|        product_name|product_price|
+-------------------+--------------------+-------------+
|        Accessories| SOLE E25 Elliptical|       999.99|
|        Accessories|Nike Kids' Grade ...|        99.99|
|        Accessories|Nike Women's Free...|        99.99|
|        Accessories|Nike Men's Free 5...|        99.99|
|        Accessories|Nike Men's Free 5...|        99.99|
|        Accessories|Fitbit Flex Wirel...|        99.95|
|        Accessories|   Nike+ Fuelband SE|         99.0|
|        Accessories|adidas Men's Germ...|         90.0|
|        Accessories|adidas Men's Germ...|         90.0|
|        Accessories|Brooks Women's Gh...|        89.99|
|        Accessories|Brooks Women's Ad...|        89.99|
|        Accessories|Brooks Men's Adre...|        89.99|
|        Accessories|Nike Women's Free...|        89.99|
|        Accessories|adidas Men's 2014...|         85.0|
|        Accessories|Coleman River Gor...|        79.98|
|        Accessories|TYR Girls' Phoeni...|        75.99|
|        Accessories|TYR Girls' Palisa...|        75.99|
|        Accessories|adidas Youth Germ...|         70.0|
|        Accessories|Lotto Men's Zhero...|        59.99|
|        Accessories|Under Armour Wome...|        54.97|
|        Accessories|adidas Men's Germ...|         50.0|
|        Accessories|Slazenger Women's...|         32.0|
|        Accessories|Nike Women's Temp...|         30.0|
|        Accessories|Nike Men's Deutsc...|         30.0|
|        Accessories|Kijaro Dual Lock ...|        29.99|
|        Accessories|adidas Women's 20...|         28.0|
|        Accessories|adidas Men's 2014...|         28.0|
|        Accessories|adidas Men's 2014...|         28.0|
|        Accessories|"Nike Women's Pro...|         28.0|
|        Accessories|"adidas Original ...|         28.0|
|        Accessories|adidas Men's Germ...|         25.0|
|        Accessories|Team Golf Minneso...|        24.99|
|        Accessories|Team Golf Detroit...|        24.99|
|        Accessories|Team Golf Dallas ...|        24.99|
|        Accessories|Team Golf Chicago...|        24.99|
|        Accessories|Team Golf New Yor...|        24.99|
|        Accessories|Team Golf New Yor...|        24.99|
|        Accessories|Team Golf Baltimo...|        24.99|
|        Accessories|Team Golf San Fra...|        24.99|
|        Accessories|Team Golf West Vi...|        24.99|
|        Accessories|Team Golf Chicago...|        24.99|
|        Accessories|Team Golf Tenness...|        24.99|
|        Accessories|Team Golf Boston ...|        24.99|
|        Accessories|Team Golf Washing...|        24.99|
|        Accessories|Team Golf San Fra...|        24.99|
|        Accessories|Team Golf Oakland...|        24.99|
|        Accessories|Team Golf New Eng...|        24.99|
|        Accessories|Team Golf Penn St...|        24.99|
|        Accessories|Team Golf Wiscons...|        24.99|
|        Accessories|Team Golf Missour...|        24.99|
|        Accessories|Team Golf Green B...|        24.99|
|        Accessories|Team Golf South C...|        24.99|
|        Accessories|Team Golf St. Lou...|        24.99|
|        Accessories|Team Golf Pittsbu...|        24.99|
|        Accessories|Team Golf Texas L...|        24.99|
|        Accessories|adidas Men's 2014...|         24.0|
|        Accessories|adidas Men's 2014...|         22.0|
|        Accessories|adidas Men's Germ...|         22.0|
|        Accessories|adidas Men's 2014...|         22.0|
|        Accessories|Nike Women's Pro ...|        21.99|
|        Accessories|adidas Men's 2014...|         20.0|
|        Accessories|adidas Men's 2014...|         20.0|
|        Accessories|adidas Youth Germ...|         18.0|
|        Accessories|  SOLE F85 Treadmill|      1799.99|
|        Accessories|"Top Flite Women'...|       179.98|
|        Accessories|Jordan Men's VI R...|       134.99|
|    As Seen on  TV!|PRIMED 7' x 5' So...|        99.99|
|    As Seen on  TV!|Nike Men's Free T...|        99.99|
|    As Seen on  TV!|Nike Men's Free T...|        99.99|
|    As Seen on  TV!|Elevation Trainin...|        79.99|
|    As Seen on  TV!|Reebok Women's So...|        64.99|
|    As Seen on  TV!|Nike Men's Comfor...|        44.99|
|    As Seen on  TV!|Fitness Gear Pro ...|       349.99|
|    As Seen on  TV!|Under Armour Men'...|        34.99|
|    As Seen on  TV!|Under Armour Men'...|        34.99|
|    As Seen on  TV!|Under Armour Wome...|        31.99|
|    As Seen on  TV!|Teeter Hang Ups N...|       299.99|
|    As Seen on  TV!|McDavid HEX Exten...|        29.99|
|    As Seen on  TV!|adidas Brazuca Fi...|        29.99|
|    As Seen on  TV!|Under Armour Kids...|        27.99|
|    As Seen on  TV!|Fitness Gear Pro ...|       249.99|
|    As Seen on  TV!|Fitness Gear 300 ...|       209.99|
|    As Seen on  TV!|Fitness Gear Pro ...|       179.99|
|    As Seen on  TV!|Jugs Complete Pra...|       149.99|
|    As Seen on  TV!|SKLZ Quickster 12...|       149.99|
|    As Seen on  TV!|PRIMED 12' x 6' I...|       149.99|
|    As Seen on  TV!|Nike Men's Finger...|       124.99|
|    As Seen on  TV!|Franklin 12' x 6'...|       119.99|
|Baseball & Softball|   Nike+ Fuelband SE|         99.0|
|Baseball & Softball|adidas Men's Germ...|         90.0|
|Baseball & Softball|Nike Men's USA Aw...|         90.0|
|Baseball & Softball|adidas Men's Germ...|         90.0|
|Baseball & Softball|Nike Men's USA Wh...|         90.0|
|Baseball & Softball|adidas Men's Mexi...|         90.0|
|Baseball & Softball|Nike Youth USA Aw...|         75.0|
|Baseball & Softball|adidas Men's F10 ...|        59.99|
|Baseball & Softball|adidas Men's F10 ...|        59.99|
|Baseball & Softball|Quest Q64 10 FT. ...|        59.98|
|Baseball & Softball|adidas Kids' F10 ...|        44.99|
|Baseball & Softball|adidas Brazuca 20...|        39.99|
+-------------------+--------------------+-------------+


