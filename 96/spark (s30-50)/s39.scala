Problem Scenario 39 : You have been given two files 
spark16/file1.txt 
spark16/file2.txt 
1,9,5
2,7,4
3,8,3
Spark16/file2.txt
1,g,h
2,I,j
3,k,l
Load these two files as Spark RDD and join them to produce the below results 
(1, ( (9,5), (g,h) )) 
(2, ( (7,4), (I,j) )) 
(3, ( (8,3), (k,l) )) 
And write code snippet which will sum the second columns of above joined results (5+4+3). 

==================================================================================
Solution : 
Step 1 : Create files in hdfs using Hue. 
Step 2 : Create pairRDD tor both the files. 


scala> val file1 = sc.textFile("spark16/file1.txt")
scala> val file2 = sc.textFile("spark16/file2.txt")

val file1Map = file1.map(x=> (x.split(",")(0), (x.split(",")(1),x.split(",")(2))) )
scala> file1Map.first
res36: (String, (String, String)) = (1,(9,5))


val file2Map = file2.map(x=> (x.split(",")(0), (x.split(",")(1),x.split(",")(2))) )
scala> file2Map.first
res37: (String, (String, String)) = (1,(g,h))


val join = file1Map.join(file2Map)

res41: Array[(String, ((String, String), (String, String)))] = Array((2,((7,4),(I,j))), (3,((8,3),(k,l))), (1,((9,5),(g,h))))

val sorted = join.sortBy(_._1)
res42: Array[(String, ((String, String), (String, String)))] = Array((1,((9,5),(g,h))), (2,((7,4),(I,j))), (3,((8,3),(k,l))))
