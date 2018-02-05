Problem Scenario 55 : You have been given below code snippet. 
val pairRDD1 = sc.parallelize(List( ("cat",2), ("cat", 5), ("book", 4),("cat", 12))) 
val pairRDD2 = sc.parallelize(List( ("cat" ,2), ("cup", 5), ("mouse", 4),("cat", 12))) 
operation 1 
Write a correct code snippet for operation1 which will produce desired output, shown below. 
Array[(String, (Option[lnt], Option[lnt]))] = Array((book,(Some(4),None)), (mouse,(None,Some(4))), (cup,(None,Some(5))), (cat,(Some(2),Some(2))), (cat,(Some(2),Some(12))), (cat,(Some(5),Some(2))), (cat,(Some(5),Some(12))), (cat,(Some(112),Some(2))), (cat,(Some(12),Some(12))),

================================================================================

Solution : 
pairRDD1.fullouter.Join(pairRDD2).collect 
fullOuterJoin [Pair] 
Performs the full outer join between two paired RDDS. 
Listing Variants 
def fullOuterJoin[W](other:RDD[(K,W)],numPartitions:Int):RDD[(K,(Option[V],Option[W]))]
def fullOuterJoin[W](other:RDD[(K,W)]:RDD[(K,(Option[V],Option[W]))]
def fullOuterJoin[W](other:RDD[(K,W)],partitioner:partitioner):RDD[(K,(Option[V],Option[W]))
