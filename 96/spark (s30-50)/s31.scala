Problem Scenario 31 : You have given following two files 
1. Content.txt : Contain a huge text file containing space separated words. 
2. Remove.txt : Ignore/filter all the words given in this file (Comma Separated). 
Write a Spark program which reads the Content.txt tile and load as an RDD, remove all the words from a broadcast variables (which is loaded as an RDD of words from remove.txt). and count the occurrence of the each word and save ie as a text file  HDFS.

==================================================================
Solution : 
Step 1 : Create all three files in hdfs in directory called spark2 (We will do using Hue). However, you can first create in local filesystem and then upload it to hdfs.

[paslechoix@gw01 data]$ hdfs dfs -ls spark2
Found 2 items
-rw-r--r--   3 paslechoix hdfs      22756 2018-02-04 22:15 spark2/Peter.txt
-rw-r--r--   3 paslechoix hdfs       2912 2018-02-04 22:15 spark2/stopwordlist.txt

Step 2 : Load the Content.txt file 
val Peter = sc.textFile("spark2/Peter.txt") //Load the text file 

scala> Peter.count
res0: Long = 167

scala> Peter.take(10).foreach(println)
1 Peter, an apostle of Jesus Christ, to the strangers scattered throughout Pontus, Galatia, Cappadocia, Asia, and Bithynia,
2 Elect according to the foreknowledge of God the Father, through sanctification of the Spirit, unto obedience and sprinkling of the blood of Jesus Christ: Grace unto you, and peace, be multiplied.
3 Blessed be the God and Father of our Lord Jesus Christ, which according to his abundant mercy hath begotten us again unto a lively hope by the resurrection of Jesus Christ from the dead,
4 To an inheritance incorruptible, and undefiled, and that fadeth not away, reserved in heaven for you,
5 Who are kept by the power of God through faith unto salvation ready to be revealed in the last time.
6 Wherein ye greatly rejoice, though now for a season, if need be, ye are in heaviness through manifold temptations:
7 That the trial of your faith, being much more precious than of gold that perisheth, though it be tried with fire, might be found unto praise and honour and glory at the appearing of Jesus Christ:
8 Whom having not seen, ye love; in whom, though now ye see him not, yet believing, ye rejoice with joy unspeakable and full of glory:
9 Receiving the end of your faith, even the salvation of your souls.
10 Of which salvation the prophets have inquired and searched diligently, who prophesied of the grace that should come unto you:

scala> val peterRDD = peter.flatMap(x=>x.split(" ")).map(w=>w.trim)
scala> peterRDD.count
res3: Long = 4196
scala> peterRDD.take(10).foreach(println)
1
Peter,
an
apostle
of
Jesus
Christ,
to
the
strangers



Step 3 : Load the Remove.txt file 
scala> val stop = sc.textFile("spark2/stopwordlist.txt")

scala> val stopRDD = stop.flatmap(x=>x.split(",")).map(w=>w.trim)
stopRDD.count 
res10: Long = 429


scala> val exclude_stop = peterRDD.subtract(stopRDD)

scala> exclude_stop.count
res32: Long = 2032



Step 8 : Create a PairRDD, so we can have (word,1) tuple or PairRDD. 
val result = exclude_stop.map(word => (word, 1)) 

scala> result.take(10).foreach(println)
(,1)
(exceeding,1)
(exceeding,1)
(bring,1)
(bring,1)
(bring,1)
(bring,1)
(preached,1)
(preached,1)
(preached,1)


Step 9 : Now do the word count on PairRDD. 
val wordCount = result.reduceByKey(_ + _) 

scala> wordCount.take(10).foreach(println)
(God,25)
(call,1)
(offer,1)
(light:,1)
(Because,1)
(joy.,1)
(foreordained,1)
(blemish,1)
(themselves,,2)
(ark,1)

Step 9.1: Sort the result 
val sorted = wordCount.sortBy(_._2, false)

scala> sorted.take(10).foreach(println)
(ye,59)
(unto,49)
(God,25)
(For,24)
(But,20)
(Lord,19)
(And,19)
(Jesus,18)
(I,14)
(hath,14)


Step 10 : Save the output as a Text file. 
sorted.saveAsTextFile("spark2/sorted_peter.txt") 


[paslechoix@gw03 data]$ hdfs dfs -ls spark2/sorted_peter.txt/ *
-rw-r--r--   3 paslechoix hdfs          0 2018-02-26 21:59 spark2/sorted_peter.txt/_SUCCESS
-rw-r--r--   3 paslechoix hdfs       3194 2018-02-26 21:59 spark2/sorted_peter.txt/part-00000
-rw-r--r--   3 paslechoix hdfs       9882 2018-02-26 21:59 spark2/sorted_peter.txt/part-00001


[paslechoix@gw03 data]$ hdfs dfs -copyToLocal spark2/sorted_peter.txt/part-*

[paslechoix@gw03 data]$ ls part*
part-00000  part-00001

[paslechoix@gw03 data]$ cat part-00000 part-00001 > sorted.txt

[paslechoix@gw03 data]$ head -20 sorted.txt
(ye,59)
(unto,49)
(God,25)
(For,24)
(But,20)
(Lord,19)
(And,19)
(Jesus,18)
(I,14)
(hath,14)
(glory,12)
(day,11)
(according,11)
(you,,11)
(own,10)
(Christ,,10)
(evil,9)
(holy,9)
(God,,9)
(time,9)
[paslechoix@gw03 data]$


