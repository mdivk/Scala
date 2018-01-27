// Ranking - Get top N priced products with in each product category

val products = sc.textFile("/public/retail_db/products")
val productsMap = products.
  filter(product => product.split(",")(4) != "").
  map(product => (product.split(",")(1).toInt, product))
  
scala> products.first  
res0: String = 1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy
  
val productsGroupByCategory = productsMap.groupByKey

scala> productsGroupByCategory.first
res1: (String, Iterable[String]) = (219.0,CompactBuffer(674,31,PING G30 Hybrid,,219.0,http://images.acmesports.sports/PING+G30+Hybrid))


def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {
  val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
  val topNPrices = productPrices.toList.sortBy(p => -p).take(topN)

  val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
  val minOfTopNPrices = topNPrices.min

  val topNPricedProducts = productsSorted.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)

  topNPricedProducts
}

val top3PricedProductsPerCategory = productsGroupByCategory.flatMap(rec => getTopNPricedProducts(rec._2, 3))


674,31,PING G30 Hybrid,,219.0,http://images.acmesports.sports/PING+G30+Hybrid
2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat
10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
17,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
20,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
84,5,Nike Men's KD VI Basketball Shoe,,129.99,http://images.acmesports.sports/Nike+Men%27s+KD+VI+Basketball+Shoe
173,9,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
234,11,Fitness Gear Pro Core Bench,,129.99,http://images.acmesports.sports/Fitness+Gear+Pro+Core+Bench
292,38,Garmin Women's Forerunner 10 GPS Watch,,129.99,http://images.acmesports.sports/Garmin+Women%27s+Forerunner+10+GPS+Watch

val top2ricedProductsPerCategory = productsGroupByCategory.flatMap(rec => getTopNPricedProducts(rec._2, 2))

674,31,PING G30 Hybrid,,219.0,http://images.acmesports.sports/PING+G30+Hybrid
2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat
10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
17,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
20,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
84,5,Nike Men's KD VI Basketball Shoe,,129.99,http://images.acmesports.sports/Nike+Men%27s+KD+VI+Basketball+Shoe
173,9,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
234,11,Fitness Gear Pro Core Bench,,129.99,http://images.acmesports.sports/Fitness+Gear+Pro+Core+Bench
292,38,Garmin Women's Forerunner 10 GPS Watch,,129.99,http://images.acmesports.sports/Garmin+Women%27s+Forerunner+10+GPS+Watch


val productsMap = products.
      filter(p=>p.split(",")(4)!="").
      map(pd=> (pd.split(",")(4), pd))
