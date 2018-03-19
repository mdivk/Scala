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
res49: String = 1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy

/* I don't know how to use the class which seems to be very helpful, can you help rewrite the code with the class? Thank you so much.
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
*/

val prd = productsf.map(rec => (rec.split(","))).map(line=>(line(0).toInt, line(1).toInt, line(2), line(4).toFloat))
res48: (Int, Int, String, Float) = (1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)

RDD prd includes: (product_id, product_category_id, product_name, product_price)

val sorted = prd.sortBy(rec => (rec._1, -rec._2))

RDD sorted is sorted by product_category_id, and then price in desc, from the result below it shows the second sortBy option is NOT working 
sorted.take(100).foreach(println)
(1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(2,2,Under Armour Men's Highlight MC Football Clea,129.99)
(3,2,Under Armour Men's Renegade D Mid Football Cl,89.99)
(4,2,Under Armour Men's Renegade D Mid Football Cl,89.99)
(5,2,Riddell Youth Revolution Speed Custom Footbal,199.99)
(6,2,Jordan Men's VI Retro TD Football Cleat,134.99)
(7,2,Schutt Youth Recruit Hybrid Custom Football H,99.99)
(8,2,Nike Men's Vapor Carbon Elite TD Football Cle,129.99)
(9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,50.0)
(10,2,Under Armour Men's Highlight MC Football Clea,129.99)
(11,2,Fitness Gear 300 lb Olympic Weight Set,209.99)
(12,2,Under Armour Men's Highlight MC Alter Ego Fla,139.99)
(13,2,Under Armour Men's Renegade D Mid Football Cl,89.99)
(14,2,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)
(15,2,Under Armour Kids' Highlight RM Alter Ego Sup,59.99)
(16,2,Riddell Youth 360 Custom Football Helmet,299.99)
(17,2,Under Armour Men's Highlight MC Football Clea,129.99)
(18,2,Reebok Men's Full Zip Training Jacket,29.97)
(19,2,Nike Men's Fingertrap Max Training Shoe,124.99)
(20,2,Under Armour Men's Highlight MC Football Clea,129.99)
(21,2,Under Armour Kids' Highlight RM Football Clea,54.99)
(22,2,Kijaro Dual Lock Chair,29.99)
(23,2,Under Armour Men's Highlight MC Alter Ego Hul,139.99)
(24,2,Elevation Training Mask 2.0,79.99)
(25,3,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(26,3,Nike Men's USA White Home Stadium Soccer Jers,90.0)
(27,3,Nike Youth USA Away Stadium Replica Soccer Je,75.0)
(28,3,adidas Brazuca 2014 Top Glider Soccer Ball,29.99)
(29,3,Nike Men's USA Away Stadium Replica Soccer Je,90.0)
(30,3,adidas Men's Germany Home Soccer Jersey,90.0)
(31,3,Nike+ Fuelband SE,99.0)
(32,3,PUMA Men's evoPOWER 1 Tricks FG Soccer Cleat,189.99)
(33,3,adidas Brazuca 2014 Top Repliqué Soccer Ball,39.99)
(34,3,Nike Women's Pro Core 3" Compression Shorts,28.0)
(35,3,adidas Brazuca 2014 Official Match Ball,159.99)
(36,3,adidas Men's Germany Black/Red Away Match Soc,90.0)
(37,3,adidas Kids' F5 Messi FG Soccer Cleat,34.99)
(38,3,Nike Men's Hypervenom Phantom Premium FG Socc,0.0)
(39,3,Nike Women's Pro Victory Compression Bra,21.99)
(40,3,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)
(41,3,adidas Men's Mexico Home Soccer Jersey,90.0)
(42,3,adidas Kids' F10 Messi TRX FG Soccer Cleat,44.99)
(43,3,Kijaro Dual Lock Chair,29.99)
(44,3,adidas Men's F10 Messi TRX FG Soccer Cleat,59.99)
(45,3,adidas Men's F10 Messi FG Soccer Cleat,59.99)
(46,3,Quest 12' x 12' Dome Canopy,149.99)
(47,3,Nike Women's Pro Hyperwarm Fitted Tights,24.97)
(48,3,adidas Brazuca Final Rio Official Match Ball,159.99)
(49,4,Diamondback Adult Sorrento Mountain Bike 2014,299.98)
(50,4,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(51,4,MAC Sports Collapsible Wagon,69.99)
(52,4,Easton Mako Youth Bat 2014 (-11),249.97)
(53,4,adidas Brazuca 2014 Top Glider Soccer Ball,29.99)
(54,4,Nike+ Fuelband SE,99.0)
(55,4,adidas Brazuca 2014 Top Repliqué Soccer Ball,39.99)
(56,4,Fitbit Flex Wireless Activity & Sleep Wristba,99.95)
(57,4,Nike Women's Pro Core 3" Compression Shorts,28.0)
(58,4,Diamondback Boys' Insight 24 Performance Hybr,299.99)
(59,4,adidas Brazuca 2014 Official Match Ball,159.99)
(60,4,SOLE E25 Elliptical,999.99)
(61,4,Diamondback Girls' Clarity 24 Hybrid Bike 201,299.99)
(62,4,Easton XL1 Youth Bat 2014 (-10),179.97)
(63,4,Fitness Gear 300 lb Olympic Weight Set,209.99)
(64,4,Nike Women's Pro Victory Compression Bra,21.99)
(65,4,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)
(66,4,SOLE F85 Treadmill,1799.99)
(67,4,Kijaro Dual Lock Chair,29.99)
(68,4,Diamondback Adult Outlook Mountain Bike 2014,309.99)
(69,4,Easton S1 Youth Bat 2014 (-12),179.97)
(70,4,Elevation Training Mask 2.0,79.99)
(71,4,Diamondback Adult Response XE Mountain Bike 2,349.98)
(72,4,Quest 12' x 12' Dome Canopy,149.99)
(73,5,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(74,5,Goaliath 54" In-Ground Basketball Hoop with P,499.99)
(75,5,Nike Women's Pro Core 3" Compression Shorts,28.0)
(76,5,Jordan Men's VI Retro TD Football Cleat,134.99)
(77,5,Schutt Youth Recruit Hybrid Custom Football H,99.99)
(78,5,Nike Kids' Grade School KD VI Basketball Shoe,99.99)
(79,5,Fitness Gear 300 lb Olympic Weight Set,209.99)
(80,5,Nike Women's Pro Victory Compression Bra,21.99)
(81,5,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)
(82,5,Kijaro Dual Lock Chair,29.99)
(83,5,Elevation Training Mask 2.0,79.99)
(84,5,Nike Men's KD VI Basketball Shoe,129.99)
(85,5,Nike Kids' Grade School LeBron XI Basketball ,139.99)
(86,5,Quest 12' x 12' Dome Canopy,149.99)
(87,5,Nike Women's Pro Hyperwarm Fitted Tights,24.97)
(88,5,Nike Kids' Grade School KD VI Basketball Shoe,99.99)
(89,5,Nike Elite Crew Basketball Sock,14.0)
(90,5,Nike Men's LeBron XI Basketball Shoe,199.99)
(91,5,Quest Q100 10' X 10' Dome Canopy,99.98)
(92,5,Nike Men's LeBron XI Low Basketball Shoe,169.99)
(93,5,Under Armour Men's Tech II T-Shirt,24.99)
(94,5,Fitness Gear Pro Utility Bench,179.99)
(95,5,Nike Hoops Elite Team Backpack,70.0)
(96,5,Teeter Hang Ups NXT-S Inversion Table,299.99)
(97,6,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(98,6,Nike Women's Pro Core 3" Compression Shorts,28.0)
(99,6,Nike Women's Pro Victory Compression Bra,21.99)
(100,6,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)

in mysql:

select product_id, product_category_id, product_price
from 
(
   select product_id, product_category_id, product_price,
      (@num:=if(@group = product_category_id, @num +1, if(@group := product_category_id, 1, 1))) row_number 
  from products t
  CROSS JOIN (select @num:=0, @group:=null) c
  order by product_category_id, product_price desc, product_id
) as x 
where x.row_number <= 3;

+------------+---------------------+---------------+
| product_id | product_category_id | product_price |
+------------+---------------------+---------------+
|         16 |                   2 |        299.99 |
|         11 |                   2 |        209.99 |
|          5 |                   2 |        199.99 |
|         40 |                   3 |        199.99 |
|         32 |                   3 |        189.99 |
|         35 |                   3 |        159.99 |
|         66 |                   4 |       1799.99 |
|         60 |                   4 |        999.99 |
|         71 |                   4 |        349.98 |
|         74 |                   5 |        499.99 |
|         96 |                   5 |        299.99 |
|         79 |                   5 |        209.99 |
|        117 |                   6 |        399.99 |
|        106 |                   6 |        299.99 |
|        100 |                   6 |        199.99 |
|        127 |                   7 |        329.99 |
|        137 |                   7 |        299.99 |
|        144 |                   7 |        249.99 |
|        162 |                   8 |        399.99 |
|        153 |                   8 |        299.99 |
|        148 |                   8 |        199.99 |
|        185 |                   9 |        499.99 |
|        181 |                   9 |        399.99 |
|        169 |                   9 |        299.98 |
|        208 |                  10 |       1999.99 |
|        199 |                  10 |       1799.99 |
|        197 |                  10 |        999.99 |
|        229 |                  11 |        799.99 |
|        226 |                  11 |        599.99 |
|        225 |                  11 |        499.99 |
|        262 |                  12 |        179.99 |
|        255 |                  12 |        139.99 |
|        260 |                  12 |        119.99 |
|        268 |                  13 |        199.99 |
|        281 |                  13 |         99.99 |
|        265 |                  13 |         79.99 |
|        320 |                  15 |           100 |
|        322 |                  15 |         81.99 |
|        325 |                  15 |            79 |
|        346 |                  16 |        349.99 |
|        343 |                  16 |        299.99 |
|        345 |                  16 |        249.99 |
|        371 |                  17 |        399.99 |
|        364 |                  17 |        299.99 |
|        366 |                  17 |        299.99 |
|        382 |                  18 |        189.99 |
|        390 |                  18 |        139.99 |
|        399 |                  18 |        139.99 |
|        407 |                  19 |        189.99 |
|        425 |                  19 |        149.99 |
|        417 |                  19 |        139.99 |
|        450 |                  20 |        249.99 |
|        452 |                  20 |        169.99 |
|        437 |                  20 |        139.99 |
|        458 |                  21 |        139.99 |
|        453 |                  21 |         99.99 |
|        463 |                  21 |         99.99 |
|        496 |                  22 |       1799.99 |
|        488 |                  22 |        999.99 |
|        481 |                  22 |        179.98 |
|        516 |                  24 |        229.99 |
|        508 |                  24 |        189.99 |
|        513 |                  24 |        159.99 |
|        545 |                  25 |        229.99 |
|        534 |                  25 |        189.99 |
|        536 |                  25 |        179.98 |
|        566 |                  26 |            90 |
|        569 |                  26 |            90 |
|        560 |                  26 |            85 |
|        590 |                  27 |            90 |
|        593 |                  27 |            90 |
|        584 |                  27 |            85 |
|        625 |                  29 |        199.99 |
|        629 |                  29 |        169.99 |
|        623 |                  29 |        149.99 |
|        663 |                  30 |        139.99 |
|        664 |                  30 |        139.99 |
|        665 |                  30 |        139.99 |
|        685 |                  31 |        899.99 |
|        676 |                  31 |           899 |
|        675 |                  31 |           799 |
|        694 |                  32 |        999.99 |
|        695 |                  32 |        999.99 |
|        698 |                  32 |        699.99 |
|        739 |                  33 |        169.99 |
|        740 |                  33 |        169.99 |
|        718 |                  33 |        139.99 |
|        743 |                  34 |        169.99 |
|        744 |                  34 |        169.99 |
|        745 |                  34 |        149.99 |
|        766 |                  35 |        299.99 |
|        768 |                  35 |        299.99 |
|        773 |                  35 |        249.99 |
|        791 |                  36 |         24.99 |
|        794 |                  36 |         24.99 |
|        806 |                  36 |         24.99 |
|        817 |                  37 |         51.99 |
|        819 |                  37 |         51.99 |
|        821 |                  37 |         51.99 |
|        860 |                  38 |        599.99 |
|        300 |                  38 |        449.99 |
|        307 |                  38 |        449.99 |
|        861 |                  39 |        199.99 |
|        862 |                  39 |        179.99 |
|        880 |                  39 |        139.99 |
|        885 |                  40 |         24.99 |
|        886 |                  40 |         24.99 |
|        887 |                  40 |         24.99 |
|        601 |                  41 |        399.99 |
|        606 |                  41 |        299.99 |
|        607 |                  41 |        249.99 |
|        933 |                  42 |        179.99 |
|        935 |                  42 |        169.99 |
|        937 |                  42 |        169.99 |
|        953 |                  43 |        449.99 |
|        944 |                  43 |        399.99 |
|        955 |                  43 |        399.99 |
|        978 |                  44 |        399.99 |
|        983 |                  44 |        249.99 |
|        966 |                  44 |        199.99 |
|       1009 |                  45 |        599.99 |
|        987 |                  45 |        399.99 |
|        995 |                  45 |        399.99 |
|       1016 |                  46 |        549.99 |
|       1020 |                  46 |        549.99 |
|       1011 |                  46 |        499.99 |
|       1048 |                  47 |       1099.99 |
|       1054 |                  47 |        699.99 |
|       1035 |                  47 |        499.99 |
|       1069 |                  48 |        799.99 |
|       1067 |                  48 |        549.99 |
|       1074 |                  48 |        549.99 |
|       1084 |                  49 |        399.99 |
|       1104 |                  49 |        399.99 |
|       1085 |                  49 |        349.99 |
|       1116 |                  50 |           130 |
|       1117 |                  50 |           130 |
|       1118 |                  50 |           130 |
|       1136 |                  51 |        219.99 |
|       1138 |                  51 |        219.99 |
|       1139 |                  51 |        219.99 |
|       1154 |                  52 |           170 |
|       1156 |                  52 |           170 |
|       1157 |                  52 |           170 |
|       1182 |                  53 |        199.99 |
|       1192 |                  53 |        199.99 |
|       1193 |                  53 |        199.99 |
|       1214 |                  54 |        299.99 |
|       1216 |                  54 |        299.99 |
|       1210 |                  54 |        199.99 |
|       1226 |                  55 |            85 |
|       1227 |                  55 |            85 |
|       1235 |                  55 |            85 |
|       1256 |                  56 |        159.99 |
|       1259 |                  56 |        159.99 |
|       1250 |                  56 |            90 |
|       1279 |                  57 |        189.99 |
|       1297 |                  57 |        174.99 |
|       1281 |                  57 |        159.99 |
|       1319 |                  58 |           241 |
|       1299 |                  58 |           194 |
|       1302 |                  58 |           130 |
|       1323 |                  59 |           100 |
|       1330 |                  59 |           100 |
|       1331 |                  59 |           100 |
+------------+---------------------+---------------+


import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
val keyByCategory = prd.keyBy(_._1)

as the class Info was not created successfully earlier, the following command is expected to fail:
val topByKey: RDD[(Int, Array[Info])] = keyByCategory.topByKey(3)

changed to:
val topByKey = keyByCategory.topByKey(3)

scala> val topByKey = keyByCategory.topByKey(3)
(778,[Lscala.Tuple4;@21eeb0ce)
(386,[Lscala.Tuple4;@29af0d3b)
(454,[Lscala.Tuple4;@15682e7f)
(1084,[Lscala.Tuple4;@74e3836a)
(1110,[Lscala.Tuple4;@20df25f8)
(1260,[Lscala.Tuple4;@7148320d)
(772,[Lscala.Tuple4;@2587f18d)
(324,[Lscala.Tuple4;@4d17ce84)
(180,[Lscala.Tuple4;@143ec23b)
(1080,[Lscala.Tuple4;@24d642eb)


val topWithKeysSorted = topByKey.sortBy(_._1)
(1,[Lscala.Tuple4;@1788be37)
(2,[Lscala.Tuple4;@58ae7e50)
(3,[Lscala.Tuple4;@1d74720f)
(4,[Lscala.Tuple4;@53222524)
(5,[Lscala.Tuple4;@1894071)
(6,[Lscala.Tuple4;@2afee2ee)
(7,[Lscala.Tuple4;@142409b4)
(8,[Lscala.Tuple4;@56ed0511)
(9,[Lscala.Tuple4;@1d2c5d75)
(10,[Lscala.Tuple4;@401835ac)

val result = topWithKeysSorted.map{ case (k, Array((v1, v2, v3, v4))) => (k, v1, v2, v3, v4) }
(1,1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(2,2,2,Under Armour Men's Highlight MC Football Clea,129.99)
(3,3,2,Under Armour Men's Renegade D Mid Football Cl,89.99)
(4,4,2,Under Armour Men's Renegade D Mid Football Cl,89.99)
(5,5,2,Riddell Youth Revolution Speed Custom Footbal,199.99)
(6,6,2,Jordan Men's VI Retro TD Football Cleat,134.99)
(7,7,2,Schutt Youth Recruit Hybrid Custom Football H,99.99)
(8,8,2,Nike Men's Vapor Carbon Elite TD Football Cle,129.99)
(9,9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,50.0)
(10,10,2,Under Armour Men's Highlight MC Football Clea,129.99)
(11,11,2,Fitness Gear 300 lb Olympic Weight Set,209.99)
(12,12,2,Under Armour Men's Highlight MC Alter Ego Fla,139.99)
(13,13,2,Under Armour Men's Renegade D Mid Football Cl,89.99)
(14,14,2,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)
(15,15,2,Under Armour Kids' Highlight RM Alter Ego Sup,59.99)
(16,16,2,Riddell Youth 360 Custom Football Helmet,299.99)
(17,17,2,Under Armour Men's Highlight MC Football Clea,129.99)
(18,18,2,Reebok Men's Full Zip Training Jacket,29.97)
(19,19,2,Nike Men's Fingertrap Max Training Shoe,124.99)
(20,20,2,Under Armour Men's Highlight MC Football Clea,129.99)
(21,21,2,Under Armour Kids' Highlight RM Football Clea,54.99)
(22,22,2,Kijaro Dual Lock Chair,29.99)
(23,23,2,Under Armour Men's Highlight MC Alter Ego Hul,139.99)
(24,24,2,Elevation Training Mask 2.0,79.99)
(25,25,3,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(26,26,3,Nike Men's USA White Home Stadium Soccer Jers,90.0)
(27,27,3,Nike Youth USA Away Stadium Replica Soccer Je,75.0)
(28,28,3,adidas Brazuca 2014 Top Glider Soccer Ball,29.99)
(29,29,3,Nike Men's USA Away Stadium Replica Soccer Je,90.0)
(30,30,3,adidas Men's Germany Home Soccer Jersey,90.0)
(31,31,3,Nike+ Fuelband SE,99.0)
(32,32,3,PUMA Men's evoPOWER 1 Tricks FG Soccer Cleat,189.99)
(33,33,3,adidas Brazuca 2014 Top Repliqué Soccer Ball,39.99)
(34,34,3,Nike Women's Pro Core 3" Compression Shorts,28.0)
(35,35,3,adidas Brazuca 2014 Official Match Ball,159.99)
(36,36,3,adidas Men's Germany Black/Red Away Match Soc,90.0)
(37,37,3,adidas Kids' F5 Messi FG Soccer Cleat,34.99)
(38,38,3,Nike Men's Hypervenom Phantom Premium FG Socc,0.0)
(39,39,3,Nike Women's Pro Victory Compression Bra,21.99)
(40,40,3,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)
(41,41,3,adidas Men's Mexico Home Soccer Jersey,90.0)
(42,42,3,adidas Kids' F10 Messi TRX FG Soccer Cleat,44.99)
(43,43,3,Kijaro Dual Lock Chair,29.99)
(44,44,3,adidas Men's F10 Messi TRX FG Soccer Cleat,59.99)
(45,45,3,adidas Men's F10 Messi FG Soccer Cleat,59.99)
(46,46,3,Quest 12' x 12' Dome Canopy,149.99)
(47,47,3,Nike Women's Pro Hyperwarm Fitted Tights,24.97)
(48,48,3,adidas Brazuca Final Rio Official Match Ball,159.99)
(49,49,4,Diamondback Adult Sorrento Mountain Bike 2014,299.98)
(50,50,4,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(51,51,4,MAC Sports Collapsible Wagon,69.99)
(52,52,4,Easton Mako Youth Bat 2014 (-11),249.97)
(53,53,4,adidas Brazuca 2014 Top Glider Soccer Ball,29.99)
(54,54,4,Nike+ Fuelband SE,99.0)
(55,55,4,adidas Brazuca 2014 Top Repliqué Soccer Ball,39.99)
(56,56,4,Fitbit Flex Wireless Activity & Sleep Wristba,99.95)
(57,57,4,Nike Women's Pro Core 3" Compression Shorts,28.0)
(58,58,4,Diamondback Boys' Insight 24 Performance Hybr,299.99)
(59,59,4,adidas Brazuca 2014 Official Match Ball,159.99)
(60,60,4,SOLE E25 Elliptical,999.99)
(61,61,4,Diamondback Girls' Clarity 24 Hybrid Bike 201,299.99)
(62,62,4,Easton XL1 Youth Bat 2014 (-10),179.97)
(63,63,4,Fitness Gear 300 lb Olympic Weight Set,209.99)
(64,64,4,Nike Women's Pro Victory Compression Bra,21.99)
(65,65,4,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)
(66,66,4,SOLE F85 Treadmill,1799.99)
(67,67,4,Kijaro Dual Lock Chair,29.99)
(68,68,4,Diamondback Adult Outlook Mountain Bike 2014,309.99)
(69,69,4,Easton S1 Youth Bat 2014 (-12),179.97)
(70,70,4,Elevation Training Mask 2.0,79.99)
(71,71,4,Diamondback Adult Response XE Mountain Bike 2,349.98)
(72,72,4,Quest 12' x 12' Dome Canopy,149.99)
(73,73,5,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(74,74,5,Goaliath 54" In-Ground Basketball Hoop with P,499.99)
(75,75,5,Nike Women's Pro Core 3" Compression Shorts,28.0)
(76,76,5,Jordan Men's VI Retro TD Football Cleat,134.99)
(77,77,5,Schutt Youth Recruit Hybrid Custom Football H,99.99)
(78,78,5,Nike Kids' Grade School KD VI Basketball Shoe,99.99)
(79,79,5,Fitness Gear 300 lb Olympic Weight Set,209.99)
(80,80,5,Nike Women's Pro Victory Compression Bra,21.99)
(81,81,5,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)
(82,82,5,Kijaro Dual Lock Chair,29.99)
(83,83,5,Elevation Training Mask 2.0,79.99)
(84,84,5,Nike Men's KD VI Basketball Shoe,129.99)
(85,85,5,Nike Kids' Grade School LeBron XI Basketball ,139.99)
(86,86,5,Quest 12' x 12' Dome Canopy,149.99)
(87,87,5,Nike Women's Pro Hyperwarm Fitted Tights,24.97)
(88,88,5,Nike Kids' Grade School KD VI Basketball Shoe,99.99)
(89,89,5,Nike Elite Crew Basketball Sock,14.0)
(90,90,5,Nike Men's LeBron XI Basketball Shoe,199.99)
(91,91,5,Quest Q100 10' X 10' Dome Canopy,99.98)
(92,92,5,Nike Men's LeBron XI Low Basketball Shoe,169.99)
(93,93,5,Under Armour Men's Tech II T-Shirt,24.99)
(94,94,5,Fitness Gear Pro Utility Bench,179.99)
(95,95,5,Nike Hoops Elite Team Backpack,70.0)
(96,96,5,Teeter Hang Ups NXT-S Inversion Table,299.99)
(97,97,6,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,59.98)
(98,98,6,Nike Women's Pro Core 3" Compression Shorts,28.0)
(99,99,6,Nike Women's Pro Victory Compression Bra,21.99)
(100,100,6,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,199.99)



val result_simplified = topWithKeysSorted.map{ case (k, Array((v1, v2, v3, v4))) => (v1, v2, v4) }

(1,2,59.98)
(2,2,129.99)
(3,2,89.99)
(4,2,89.99)
(5,2,199.99)
(6,2,134.99)
(7,2,99.99)
(8,2,129.99)
(9,2,50.0)
(10,2,129.99)
(11,2,209.99)
(12,2,139.99)
(13,2,89.99)
(14,2,199.99)
(15,2,59.99)
(16,2,299.99)
(17,2,129.99)
(18,2,29.97)
(19,2,124.99)
(20,2,129.99)
(21,2,54.99)
(22,2,29.99)
(23,2,139.99)
(24,2,79.99)
(25,3,59.98)
(26,3,90.0)
(27,3,75.0)
(28,3,29.99)
(29,3,90.0)
(30,3,90.0)
(31,3,99.0)
(32,3,189.99)
(33,3,39.99)
(34,3,28.0)
(35,3,159.99)
(36,3,90.0)
(37,3,34.99)
(38,3,0.0)
(39,3,21.99)
(40,3,199.99)
(41,3,90.0)
(42,3,44.99)
(43,3,29.99)
(44,3,59.99)
(45,3,59.99)
(46,3,149.99)
(47,3,24.97)
(48,3,159.99)
(49,4,299.98)
(50,4,59.98)
(51,4,69.99)
(52,4,249.97)
(53,4,29.99)
(54,4,99.0)
(55,4,39.99)
(56,4,99.95)
(57,4,28.0)
(58,4,299.99)
(59,4,159.99)
(60,4,999.99)
(61,4,299.99)
(62,4,179.97)
(63,4,209.99)
(64,4,21.99)
(65,4,199.99)
(66,4,1799.99)
(67,4,29.99)
(68,4,309.99)
(69,4,179.97)
(70,4,79.99)
(71,4,349.98)
(72,4,149.99)
(73,5,59.98)
(74,5,499.99)
(75,5,28.0)
(76,5,134.99)
(77,5,99.99)
(78,5,99.99)
(79,5,209.99)
(80,5,21.99)
(81,5,199.99)
(82,5,29.99)
(83,5,79.99)
(84,5,129.99)
(85,5,139.99)
(86,5,149.99)
(87,5,24.97)
(88,5,99.99)
(89,5,14.0)
(90,5,199.99)
(91,5,99.98)
(92,5,169.99)
(93,5,24.99)
(94,5,179.99)
(95,5,70.0)
(96,5,299.99)
(97,6,59.98)
(98,6,28.0)
(99,6,21.99)
(100,6,199.99)

From the result above we can see the result is NOT as expected: sorted by product_id, more than 3 items from each category, and NOT sorted in price desc 