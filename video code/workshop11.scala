fp.map(r=>(r.split(",")(4).toFloat, r)).sortByKey(false).map(_._2).take(10).foreach(println)
