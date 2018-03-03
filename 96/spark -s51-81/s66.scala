Problem Scenario 66 : You have been given below code snippet. 

val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2) 
val b = a.keyBy(_.length) 

res4: Array[(Int, String)] = Array((3,dog), (5,tiger), (4,lion), (3,cat), (6,spider), (5,eagle))

val c = sc.parallelize(List("ant", "falcon", "squid"), 2)  
val d = c.keyBy(_.length) 

res5: Array[(Int, String)] = Array((3,ant), (6,falcon), (5,squid))

operation 1 

Write a correct code snippet for operationl which will produce desired output, shown below. 
Array((4,lion))

======================================================================== 

Solution : 
b.subtractByKey(d).collect 
res6: Array[(Int, String)] = Array((4,lion))

subtractByKey [Pair] : Very similar to subtract, but instead of supplying a function, the key-component of each pair will be automatically used as criterion for removing items from the first RDD.

Note: here the key is the first element, 4 is the only element left in the result, because it is using subtractByKey, so all pairs in b with the common key in d will be removed and that means 4 is left.
