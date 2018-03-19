Top n records from each group - Spark SQL solution

product_id int(11) NOT NULL AUTO_INCREMENT, 
product_category_id int(11) NOT NULL,
product_name varchar(45) NOT NULL,  
product_description varchar(255) NOT NULL, 
product_price float NOT NULL,
product_image varchar(255) NOT NULL

val productsRDD = sc.textFile("products").filter(x=>x.split(",")(4)!="")

val productsRDDmap = productsRDD.map(a => (a.split(",")(0).toInt, a.split(",")(1).toInt, a.split(",")(2), a.split(",")(4).toFloat))
val productsRDDmapDF = productsRDDmap.toDF("product_id","product_category_id", "product_name", "product_price")
+----------+-------------------+---------------------------------------------+-------------+
|product_id|product_category_id|product_name                                 |product_price|
+----------+-------------------+---------------------------------------------+-------------+
|1         |2                  |Quest Q64 10 FT. x 10 FT. Slant Leg Instant U|59.98        |
|2         |2                  |Under Armour Men's Highlight MC Football Clea|129.99       |
|3         |2                  |Under Armour Men's Renegade D Mid Football Cl|89.99        |
+----------+-------------------+---------------------------------------------+-------------+
only showing top 3 rows

productsRDDmapDF.registerTempTable("products")

val query = """
select product_id, product_category_id, product_price
from 
(
   select product_id, product_category_id, product_price,
      (@num:=if(@group = product_category_id, @num +1, if(@group := product_category_id, 1, 1))) row_number 
  from products t
  CROSS JOIN (select @num:=0, @group:=null) c
  order by product_category_id, product_price desc, product_id
) as x 
where x.row_number <= 3
"""

val result = sqlContext.sql(query)

org.apache.spark.sql.AnalysisException: cannot recognize input near 'num' ':' '=' in expression specification; line 5 pos 11