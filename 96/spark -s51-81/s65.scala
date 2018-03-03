Problem Scenario 65 : You have been given below code snippet. 

val a = sc.parallelize(List("dog","cat", "owl", "gnu", "ant"), 2) 
val b = sc.parallelize(1 to a.count.tolnt, 2) 

val c = a.zip(b)

res0: Array[(String, Int)] = Array((dog,1), (cat,2), (owl,3), (gnu,4), (ant,5))

operation 1 
Write a correct code snippet tor operation1 which will produce desired output, shown below. 
Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,l), (cat,2), (ant,5)) 


Solution : 

c.sortByKey(false).collect 

sortByKey [Ordered] : This function sorts the input RDD's data and stores it in a new RDD. The output ROD is a shuffled RDD because it stores data that is output by a reducer
which has been shuffled. The implementation of this function is actually very clever. First, it uses a range partitioner to partition the data in ranges.Within the shuffled RDD 
Then it sorts these ranges individually with map partitions using standard sort mechanisms. 
