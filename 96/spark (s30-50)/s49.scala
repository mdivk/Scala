Problem Scenario 49 : You have been given below code snippet (do a sum of values by key), with intermediate output. 

val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D") 
val data = sc.parallelize(keysWithValuesList) 

//Create key value pairs 
val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache() 
val initialCount = 0

Define two functions (addToCounts, sumPartitionCounts) such, which will produce following results. 

val addToCounts = (n: Int, v: String)=> n + 1 
val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2 


Define two functions (addToSet, mergePartitionSets) such, which will produce following results. 

import scala.collection. _

val addToSet = (s: mutable.HashSet[String], v: String) => s+= v 
val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2 

val initialSet = scala.collection.mutable.HashSet.empty[String] 

val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts) 

countByKey.collect 

res3: Array[(String, Int)] = Array((foo,5), (bar,3)) 

val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets) 

uniqueByKey.collect 
res4: Array[(String, scala.collection.mutable.HashSet[String])] = Array((bar,Set(C, D)), (foo,Set(B, A)))
