Problem Scenario 59 : You have been given below code snippet. 
val x = sc.parallelize(1 to 20) 
val y = sc.parallelize(10 to 30) 

operation 1 

z.collect 

Write a correct code snippet tor operation1 which will produce desired output, shown below. 

Array[Int] = Array(16, 12, 20, 13, 17, 14, 18, 10, 19, 15, 11) 

Solution : 


val x = sc.parallelize(1 to 20) 
res22: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)


val y = sc.parallelize(10 to 30) 
res23: Array[Int] = Array(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)

val z = x.intersection(y) 
res24: Array[Int] = Array(16, 17, 18, 19, 20, 10, 11, 12, 13, 14, 15)

intersection : Retürns the elements in the two RDDS which are the same. 

