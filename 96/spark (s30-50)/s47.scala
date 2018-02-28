Problem Scenario 47 : You have been given below code snippet, with intermediate output. 

val z = sc.parallelize(List(1,2,3,4,5,6), 2) 

// lets first print out the contents ot the RDD with partition labels 

def myfunc(index: Int, iter:Iterator[Int]) : Iterator[String] = { 
iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator} 

ln each run , output could be different, while solving problem assume below output only:

scala> z.mapPartitionsWithIndex(myfunc).collect
res24: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3], [partID:1, val: 4], [partID:1, val: 5], [partID:1, val: 6])

Now apply aggreate method on RDD z , with two reduce function , first will select max value in each partition and second will add all the maximum values from all parttions
Initialize the aggregate with value 5, hence expected output will be 16. 

==================================================================================

Solution : 

z.aggregate(5)(math.max(_, _), _+_)
