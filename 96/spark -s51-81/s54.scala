Problem Scenario 54 : You have been given below code snippet. 
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle")) 
val b = a.map(x => (x.length, x)) 

operation 1 

Write a correct code snippet for operation1 which will produce desired output, shown below. 

Array[(Int,String)] = Array((4,lion), (7,panther), (3,dogcat), (5,tigereagle)) 
===================================================================================

Solution : 

scala> b.collect
res16: Array[(Int, String)] = Array((3,dog), (5,tiger), (4,lion), (3,cat), (7,panther), (5,eagle))

b.foldByKey("")(_ + _).collect 
res30: Array[(Int, String)] = Array((3,dogcat), (4,lion), (5,tigereagle), (7,panther))


foldBYKey [Pair]
Very similar to fold, but performs the folding separately for each key of the RDD. This function is only available if the RDD consists of two-component tuples. 
Listing Variants 
def foldByKey(zeroValue: V)(func:(V,V)=>V):RDD[(K,V)]
def foldByKey(zeroValue: V, numpartitions: Int)(func: (V, V)=> V):RDD[(K, V)] 
def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V):RDD[(K, V)] 
