Problem Scenario 52 : You have been given below code snippet. 
val anRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
Operation_xyz 

Write a correct code snippet for Operation _xyz which will produce below output
Scala.collection.Map[Int,Long]= Map(5->1,8->1,3->1,6->1,1->6,2->3,4->2,7->1) 

==================================================================================
Solution : 

1. val anRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))

val anRDDMap = anRDD.map(x=>(x, 1))
res10: Array[(Int, Int)] = Array((1,1), (2,1), (3,1), (4,1), (5,1), (6,1), (7,1), (8,1), (2,1), (4,1), (2,1), (1,1), (1,1), (1,1), (1,1), (1,1))


val grouped = anRDDMap.groupByKey()
val grouped1 = anRDDMap.reduceByKey(_ + _)
res11: Array[(Int, Int)] = Array((1,6), (2,3), (3,1), (4,2), (5,1), (6,1), (7,1), (8,1))
(1,6)
(2,3)
(3,1)
(4,2)
(5,1)
(6,1)
(7,1)
(8,1)


b.countByValue 
val anRDDMap2 = anRDD.countByValue 
res4: scala.collection.Map[Int,Long] = Map(5 -> 1, 1 -> 6, 6 -> 1, 2 -> 3, 7 -> 1, 3 -> 1, 8 -> 1, 4 -> 2)
scala> anRDDMap2.take(10).foreach(println)
(5,1)
(1,6)
(6,1)
(2,3)
(7,1)
(3,1)
(8,1)
(4,2)


