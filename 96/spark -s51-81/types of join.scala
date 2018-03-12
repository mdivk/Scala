Problem Scenario 61 : You have been given below code snippet. 

val a = sc.parallelize(List("dog", "salmon", "rat", "elephant"), 3) 
val b = a.keyBy(_.length) 
res3: Array[(Int, String)] = Array((3,dog), (6,salmon), (3,rat), (8,elephant))

val c = sc.parallelize(List("dog", "cat", "salmon", "rabbit", "wolf", "penguin"), 3) 
val d = c.keyBy(_.length) 
res5: Array[(Int, String)] = Array((3,dog), (3,cat), (6,salmon), (6,rabbit), (4,wolf), (7,penguin))


//Join: note: same key will be put together, for example, in b, dog and rat have key of 3, in d: dog and cat have key of 3, they will be put togetehr and generate:
(3,(dog,dog)), (3,(dog,cat)), (3,(rat,dog)), (3,(rat,cat)

val joined = b.join(d)
res10: Array[(Int, (String, String))] = 
	Array(
		(6,(salmon,salmon)), (6,(salmon,rabbit)), (3,(dog,dog)), (3,(dog,cat)), (3,(rat,dog)), (3,(rat,cat))
	)

//Inner join: there is no inner join in RDD, use intersection indeed, which generates the common <K, V> from the two RDD

val innerjoin = b.intersection(d)
res5: Array[(Int, String)] = Array((6,salmon), (3,dog))


//Left Outer Join 

val leftouterjoin1 = b.leftOuterJoin(d)
res12: Array[(Int, (String, Option[String]))] = 
	Array(
		(6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),
		(3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(rat,Some(dog))), (3,(rat,Some(cat))), 
		(8,(elephant,None))
		)

Note: leftOuterJoin will ensure the result contains with the left RDD, that is b, so all the keys in b will be included, and the common keys in d will be joined



val leftouterjoin2 = d.leftOuterJoin(b)
res13: Array[(Int, (String, Option[String]))] = 
	Array(
		(6,(salmon,Some(salmon))), (6,(rabbit,Some(salmon))), 
		(3,(dog,Some(dog))), (3,(dog,Some(rat))), (3,(cat,Some(dog))), (3,(cat,Some(rat))), 
		(4,(wolf,None)), 
		(7,(penguin,None))
		)
Note: leftOuterJoin will ensure the result contains with the left RDD, that is d, so all the keys in d will be included, and the common keys in b will be joined

//Right Outer Join

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

//union
//simply put two RDDs together

val union = b.union(d)
res17: Array[(Int, String)] = 
	Array(
		(3,dog), (6,salmon), (3,rat), (8,elephant), (3,dog), (3,cat), (6,salmon), (6,rabbit), (4,wolf), (7,penguin)
		)

//distinct
val distinct = union.distinct
res18: Array[(Int, String)] = 
	Array(
		(7,penguin), (6,salmon), (4,wolf), (8,elephant), (3,dog), (3,cat), (3,rat), (6,rabbit)
		)

//substract
val sub1 = b.subtract(d)
res19: Array[(Int, String)] = Array((8,elephant), (3,rat))


val sub2 = d.subtract(b)
res20: Array[(Int, String)] = Array((3,cat), (4,wolf), (7,penguin), (6,rabbit))


b: Array((3,dog), (6,salmon), (3,rat), (8,elephant))
d: Array((3,dog), (3,cat), (6,salmon), (6,rabbit), (4,wolf), (7,penguin))

//cartesian
val cartesian1 = b.cartesian(d)
res25: Long = 24
res21: Array[((Int, String), (Int, String))] = 
	Array(
		((3,dog),(3,dog)), ((3,dog),(3,cat)), ((3,dog),(6,salmon)), ((3,dog),(6,rabbit)), ((3,dog),(4,wolf)), ((3,dog),(7,penguin)), 
		((6,salmon),(3,dog)), ((6,salmon),(3,cat)), ((6,salmon),(6,salmon)), ((6,salmon),(6,rabbit)), ((6,salmon),(4,wolf)), ((6,salmon),(7,penguin)), 
		((3,rat),(3,dog)), ((3,rat),(3,cat)), 
		((8,elephant),(3,dog)), ((8,elephant),(3,cat)), 
		((3,rat),(6,salmon)), ((3,rat),(6,rabbit)), 
		((8,elephant),(6,salmon)), ((8,elephant),(6,rabbit)), 
		((3,rat),(4,wolf)), 
		((3,rat),(7,penguin)), 
		((8,elephant),(4,wolf)), ((8,elephant),(7,penguin))
		)

val cartesian2 = d.cartesian(b)
res26: Long = 24
res22: Array[((Int, String), (Int, String))] = 
	Array(
		((3,dog),(3,dog)), ((3,cat),(3,dog)), ((3,dog),(6,salmon)), ((3,cat),(6,salmon)), ((3,dog),(3,rat)), ((3,dog),(8,elephant)), 
		((3,cat),(3,rat)), ((3,cat),(8,elephant)), ((6,salmon),(3,dog)), ((6,rabbit),(3,dog)), ((6,salmon),(6,salmon)), 
		((6,rabbit),(6,salmon)), ((6,salmon),(3,rat)), ((6,salmon),(8,elephant)), ((6,rabbit),(3,rat)), ((6,rabbit),(8,elephant)), 
		((4,wolf),(3,dog)), ((7,penguin),(3,dog)), ((4,wolf),(6,salmon)), ((7,penguin),(6,salmon)), ((4,wolf),(3,rat)), 
		((4,wolf),(8,elephant)), ((7,penguin),(3,rat)), ((7,penguin),(8,elephant)))

//count is the same but order is reverse



//Start the spark-shell with only one executor to see if there is any difference of cartesian result between bd and db:
[paslechoix@gw03 ~]$ spark-shell --executor-cores 1

val cartesian1 = b.cartesian(d)
res0: Array[((Int, String), (Int, String))] = 
	Array(
		((3,dog),(3,dog)), ((3,dog),(3,cat)), ((3,dog),(6,salmon)), ((3,dog),(6,rabbit)), ((3,dog),(4,wolf)), ((3,dog),(7,penguin)), 
		((6,salmon),(3,dog)), ((6,salmon),(3,cat)), ((6,salmon),(6,salmon)), ((6,salmon),(6,rabbit)), ((6,salmon),(4,wolf)), ((6,salmon),(7,penguin)), 
		((3,rat),(3,dog)), ((3,rat),(3,cat)), ((8,elephant),(3,dog)), ((8,elephant),(3,cat)), ((3,rat),(6,salmon)), ((3,rat),(6,rabbit)), 
		((8,elephant),(6,salmon)), ((8,elephant),(6,rabbit)), 
		((3,rat),(4,wolf)), ((3,rat),(7,penguin)), 
		((8,elephant),(4,wolf)), ((8,elephant),(7,penguin))
		)

val cartesian2 = d.cartesian(b)
res1: Array[((Int, String), (Int, String))] = 
	Array(
		((3,dog),(3,dog)), ((3,cat),(3,dog)), ((3,dog),(6,salmon)), ((3,cat),(6,salmon)), ((3,dog),(3,rat)), ((3,dog),(8,elephant)), ((3,cat),(3,rat)), ((3,cat),(8,elephant)), 
		((6,salmon),(3,dog)), ((6,rabbit),(3,dog)), ((6,salmon),(6,salmon)), ((6,rabbit),(6,salmon)), ((6,salmon),(3,rat)), ((6,salmon),(8,elephant)), ((6,rabbit),(3,rat)), ((6,rabbit),(8,elephant)), 
		((4,wolf),(3,dog)), 
		((7,penguin),(3,dog)), 
		((4,wolf),(6,salmon)), 
		((7,penguin),(6,salmon)), 
		((4,wolf),(3,rat)), ((4,wolf),(8,elephant)), 
		((7,penguin),(3,rat)), ((7,penguin),(8,elephant))
		)

//Conclusion: bd and db still show different order in the result, number of executors does not have impact on the result.


