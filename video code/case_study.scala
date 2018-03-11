Case study
Crime by month

Solution by RDD

val crimeData = sc.textFile("/public/crime/csv")
val header = crimeData.first
val crimeDataWithoutHeader = crimeData.filter(criminalRecord => criminalRecord != header)

val rec = crimeDataWithoutHeader.first
val distinctDates = crimeDataWithoutHeader.
	map(criminalRecord => criminalRecord.split(",")(2).split( )(0)).
	distinct.
	collect.
	sorted
	
distinctDates.foreach(println)

val t = {
 val r = rec.split(",")
 val d = r(2).split(" ")(0)
 val m = d.split("/")(2) + d.split("/")(0)
 
 ((m.toInt, r(5)),1)
}

val criminalRecordsWithMonthAndType = crimeDataWithoutHeader.
	map(rec => {
		 val r = rec.split(",")
		 val d = r(2).split(" ")(0)
		 val m = d.split("/")(2) + d.split("/")(0)
		 
		 ((m.toInt, r(5)),1)
	})
	
val crimeCountPerMonthPerType = criminalRecordsWithMonthAndType.
	reduceByKey((total, value) => total + value)

val crimeCountPerMonthPerTypeSorted = crimeCountPerMonthPerType.
	map(rec => ((rec._1._1, -rec._2), rec._1._1 + "\t" + rec._2 + "\t" + rec._1._2)).
	sortByKey()
	
val crimeCountPerMonthPerTypeSortedFinal = crimeCountPerMonthPerTypeSorted.
	map(rec => rec._2)
	
Find out the codec full name

go to hadoop core-site.xml and search for codec to get the codec name

save the result in GZip format

crimeCountPerMonthPerTypeSortedFinal.saveAsTextFile("/user/paslechoix/solutions/solution01/crimes_by_type_by_month", classOf[org.apache.hadoop.io.compress.GzipCodec])	

merge and save as one single file

crimeCountPerMonthPerTypeSortedFinal.
	coalesce(1).
	saveAsTextFile("/user/paslechoix/solutions/solution01/crimes_by_type_by_month", classOf[org.apache.hadoop.io.compress.GzipCodec])	
	