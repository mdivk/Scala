Problem Scenario 53 : You have been given below code snippet. 
val a = sc.parallelize(1 to 10, 3) 
operation1
b.collect 
Output 1 
Array[lnt] = Array(2,4,6,8,10) 
operation2 
Output 2 
Array[lnt] = Array(1,2,3) 
Write a correct code snippet tor operation1 and operation2 which will produce desired output, shown above. 

==================================================================================

Solution : 
val b = a.filter(_ % 2 == 0) 

scala> a.filter(_ %2 ==0).take(10)
res14: Array[Int] = Array(2, 4, 6, 8, 10)

scala> a.filter(_ < 4).collect 
res15: Array[Int] = Array(1, 2, 3)

filter 
Evaluates a boolean function for each data item of the RDD and puts the items for which the function returned true into the resulting RDD. 
When you provide a filter function, it must be able to handle all data items contained in the RDD. Scala provides so-called partial functions to deal with mixed data-types.(Tip:
Partial functions are very useful if you have some data which may be bad and you do not want to handle but for the good data (matching data) you want to apply some kind of map
function. The following article is good. It teaches you about partial functions in a very nice way and explains why case has to be used for partial functions:article) 
Examples for mixed data without partial functions
Val b = sc.parallelize(1 to 8)
b.filter(_<4).collect
res15:Array[Int] = Array(1,2,3)
val a = sc.parallelize(List(“cat”,”horse”,4.0,3.5,2,”dog”))
a.filter(_<4).collect
error:value<is not a member of Any
