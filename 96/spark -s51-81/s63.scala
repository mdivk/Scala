Problem Scenario 63 : You have been given below code snippet. 

val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2) 
val b = a.map(x => (x.length, x)) 
operation1

Write a correct code snippet tor operation1 which will produce desired output, shown below. 
Array[(Int,String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle)) 


Solution : 
b.collect
res0: Array[(Int, String)] = Array((3,dog), (5,tiger), (4,lion), (3,cat), (7,panther), (5,eagle))

scala> b.reduceByKey(_ + _).collect
res1: Array[(Int, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))


val c = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle", "cheetah", "horse", "wolf", "dolphin"), 2) 
val d = c.map(x=>(x.length, x))
res2: Array[(Int, String)] = Array((3,dog), (5,tiger), (4,lion), (3,cat), (7,panther), (5,eagle), (7,cheetah), (5,horse), (4,wolf), (7,dolphin))


d.reduceByKey(_ + _).collect 
res3: Array[(Int, String)] = Array((4,lionwolf), (3,dogcat), (7,panthercheetahdolphin), (5,tigereaglehorse))


reduceByKey [Pair] : This function provides the well-known reduce functionality in Spark. Please note that any function f you provide, 
should be commutative in order to generate reproducible results. 
