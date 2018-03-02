Problem Scenario 61 : You have been given below code snippet. 

val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3) 
val b = a.keyBy(_.length) 
res3: Array[(Int, String)] = Array((3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant))

val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3) 
val d = c.keyBy(_.length) 
res4: Array[(Int, String)] = Array((3,dog), (3,cat), (3,gnu), (6,salmon), (6,rabbit), (6,turkey), (4,wolf), (4,bear), (3,bee))

operation 1 

Write a correct code snippet tor operation1 which will produce desired output, shown below. 
Array[(Int,(String, Option[String]))] = Array((6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))),  
(6,(salmon,Some(rabbit))), (S,(salmon,some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(dog,Some(gnu))), (3,(dog,Some(bee))), (3,(rat,Some(dog))),
(3,(rat,Some(cat))), (3,(rat,Some(gnu))), (3,(rat,Some(bee))), (8,(elephant,None))) 

================================================================================

Solution : 

scala> b.leftOuterJoin(d).collect 
res0: Array[(Int, (String, Option[String]))] = Array((6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(dog,Some(gnu))), (3,(dog,Some(bee))), (3,(rat,Some(dog))), (3,(rat,Some(cat))), (3,(rat,Some(gnu))), (3,(rat,Some(bee))), (8,(elephant,None)))


leftOuterJoin [Pair] : Performs an left outer join using two key-value RDDS. Please note that the keys must be generally comparable to make this work correctly
keyBy : Constructs two-component tuples (key-value pairs) by applying a function on each data item. The result ot the function becomes the key and the original
data item becomes the value ot the newly created tuples. 
