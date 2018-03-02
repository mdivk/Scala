Problem Scenario 61 : You have been given below code snippet. 

val a = sc.parallelize(List("dog", "salmon", "rat", "elephant"), 3) 
val b = a.keyBy(_.length) 
res3: Array[(Int, String)] = Array((3,dog), (6,salmon), (3,rat), (8,elephant))

val c = sc.parallelize(List("dog", "cat", "salmon", "rabbit", "wolf", "penguin"), 3) 
val d = c.keyBy(_.length) 
res5: Array[(Int, String)] = Array((3,dog), (3,cat), (6,salmon), (6,rabbit), (4,wolf), (7,penguin))


Join: note: same key will be put together, for example, in b, dog and rat have key of 3, in d: dog and cat have key of 3, they will be put togetehr and generate:
(3,(dog,dog)), (3,(dog,cat)), (3,(rat,dog)), (3,(rat,cat)

val joined = b.join(d)
res10: Array[(Int, (String, String))] = 
	Array(
		(6,(salmon,salmon)), (6,(salmon,rabbit)), (3,(dog,dog)), (3,(dog,cat)), (3,(rat,dog)), (3,(rat,cat))
	)

Inner join: there is no inner join in RDD, use intersection indeed, which generates the common <K, V> from the two RDD

val innerjoin = b.Innerjoin(d)
res11: Array[(Int, String)] = Array((6,salmon), (3,dog))

Left Outer Join 

val leftouterjoin1 = b.leftOuterJoin(d)
res12: Array[(Int, (String, Option[String]))] = 
	Array(
		(6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),
		(3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(rat,Some(dog))), (3,(rat,Some(cat))), 
		(8,(elephant,None))
		)


val leftouterjoin2 = d.leftOuterJoin(b)
res13: Array[(Int, (String, Option[String]))] = 
	Array(
		(6,(salmon,Some(salmon))), (6,(rabbit,Some(salmon))), 
		(3,(dog,Some(dog))), (3,(dog,Some(rat))), (3,(cat,Some(dog))), (3,(cat,Some(rat))), 
		(4,(wolf,None)), 
		(7,(penguin,None))
		)

Right Outer Join

val rightouterjoin1 = b.rightOuterJoin(d)
res15: Array[(Int, (Option[String], String))] = 
	Array(
		(6,(Some(salmon),salmon)), (6,(Some(salmon),rabbit)), 
		(3,(Some(dog),dog)), (3,(Some(dog),cat)), (3,(Some(rat),dog)), (3,(Some(rat),cat)), 
		(4,(None,wolf)), 
		(7,(None,penguin))
	)

val rightouterjoin2 = d.rightOuterJoin(b)
res16: Array[(Int, (Option[String], String))] = 
	Array(
		(6,(Some(salmon),salmon)), (6,(Some(rabbit),salmon)), 
		(3,(Some(dog),dog)), (3,(Some(dog),rat)), (3,(Some(cat),dog)), (3,(Some(cat),rat)), 
		(8,(None,elephant))
		)


