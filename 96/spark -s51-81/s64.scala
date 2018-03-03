Problem Scenario 64 : You have been given below code snippet. 

val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3) 
val b = a.keyBy(_.length) 

val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3) 
val d = c.keyBy(_.length) 

operation 1 

Write a correct code snippet tor operation1 which will produce desired output, shown below. 
Array[(Int,(Option[String], String))] = Array((6,(Some(salmon),salmon)), (6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), (6,(Some(salmon),salmon)),
(6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), (3,(Some(dog),dog)), (3,(Some(dog),cat)), (3,(Some(dog),gnu)), (3,(Some(dog),bee)), (3,(Some(rat),dog)) 
(3,(Some(rat),cat)), (3,(Some(rat),gnu)), (3,(Some(rat),bee)), (4,(None,wolt)), (4,(None,bear))) 

Solution 

b.rightOuterJoin(d).collect 
res6: Array[(Int, (Option[String], String))] = 
	Array(
	(6,(Some(salmon),salmon)), (6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), 
	(6,(Some(salmon),salmon)), (6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), 
	(3,(Some(dog),dog)), (3,(Some(dog),cat)), (3,(Some(dog),gnu)), (3,(Some(dog),bee)), 
	(3,(Some(rat),dog)), (3,(Some(rat),cat)), (3,(Some(rat),gnu)), (3,(Some(rat),bee)), 
	(4,(None,wolf)), (4,(None,bear)))

b.join(d).collect
res8: Array[(Int, (String, String))] = 
	Array(
	(6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), 
	(3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)), (3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))


b.leftOuterJoin(d).collect
res10: Array[(Int, (String, Option[String]))] = 
	Array(
	(6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), 
	(3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(dog,Some(gnu))), (3,(dog,Some(bee))), (3,(rat,Some(dog))), (3,(rat,Some(cat))), (3,(rat,Some(gnu))), (3,(rat,Some(bee))), 
	(8,(elephant,None)))


b.intersection(d).collect
res11: Array[(Int, String)] = Array((6,salmon), (3,dog))


Note the difference between intersection and join where intersection picks up the common element and join does cross join for common element
rightOuterJoin [Pair] : Performs an right outer join using two key-value RDDS. Please note that the keys must be generally comparable to make this work correctly.
