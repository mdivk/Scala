Problem Scenario 57 : You have been given below code snippet. 
val a = sc.parallelize(1 to 9, 3) 

operation 1 

Write a correct code snippet tor operation1 which will produce desired output, shown below. 
Array[(String, Seq[lnt])] = Array((even,ArrayBuffer(2, 4, 6, 8)), (odd,ArrayBuffer(1, 3, 5, 7, 9))) 

Solution:

scala> a.groupBy(x=> if(x%2 == 0) "even" else "odd").collect

res2: Array[(String, Iterable[Int])] = Array((even,CompactBuffer(2, 4, 6, 8)), (odd,CompactBuffer(1, 3, 5, 7, 9)))

val b = a.groupBy(x=>(if(x%2==0) "even" else "odd"))
res21: Array[(String, Iterable[Int])] = Array((even,CompactBuffer(2, 4, 6, 8)), (odd,CompactBuffer(1, 3, 5, 7, 9)))
