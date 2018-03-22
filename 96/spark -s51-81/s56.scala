Problem Scenario 56 : You have been given below code snippet. 

val a = sc.parallelize(1 to 20, 4) 
operation 1 

Write a correct code snippet for operation1 which will produce desired output, shown below. 
res12: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)

Solution : 

scala> a.glom.collect
res13: Array[Array[Int]] = Array(Array(1, 2, 3, 4, 5), Array(6, 7, 8, 9, 10), Array(11, 12, 13, 14, 15), Array(16, 17, 18, 19, 20))

scala> a.glom.foreach(_.take(5).foreach(println))
1
11
12
13
14
16
15
6
7
8
9
10
2
3
4
5
17
18
19
20

scala> a.glom.foreach(_.foreach(println))
1
6
16
7
8
9
10
11
12
13
14
15
17
18
19
20
2
3
4
5


glom 
Assembles an array that contains all elements of the partition and embeds it in an RDD. Each returned array contains the contents of one partition. 
