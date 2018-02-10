Problem Scenario 70 : Write down a Spark Application using Python, 
In which it read a file "Content.txt" (On hdfs) with following content. 
Do the word count and save the results in a directory called "problem85" (On hdfs) 
Content.txt 
Hello this is HadoopExam.com 
This is QuickTechie.com 
Apache Spark Training 
This is Spark Learning Session 
Spark is faster than MapReduce 

====================================================================== 

val contentRDD = sc.textFile("Content.txt")

scala> contentRDD.take(10).foreach(println)
Hello this is HadoopExam.com
This is QuickTechie.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce


val contentRDDWordCount = contentRDD.flatMap(line => line.split(" ")).map(word =>(word,1)).reduceByKey(_ + _)

scala> contentRDDWordCount.take(20).foreach(println)
(this,1)
(is,4)
(Hello,1)
(Apache,1)
(MapReduce,1)
(Session,1)
(This,2)
(QuickTechie.com,1)
(Training,1)
(Learning,1)
(Spark,3)
(faster,1)
(than,1)
(HadoopExam.com,1)

example using Peter.txt 

val peterRDD = sc.textFile("spark2/Peter.txt")

scala> peterRDD.take(10).foreach(println)
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



val peterRDDWordCount = peterRDD.flatMap(line => line.split(" ")).map(word =>(word,1)).reduceByKey(_ + _)

scala> peterRDDWordCount.take(20).foreach(println)
(God,25)
(call,1)
(offer,1)
(light:,1)
(Because,1)
(joy.,1)
(foreordained,1)
(blemish,1)
(themselves,,2)
(greater,1)
(ark,1)
(behold,,1)
(works,,1)
(Servants,,1)
(fire,,1)
(chaste,1)
(hidden,1)
(souls:,1)
(Father,,2)
(carried,1)


sort by value in descending, if value is same then sort by key in ascending

val peterRDDWordCountSorted = peterRDDWordCount.sortBy(_._1).sortBy(_._2, ascending = false)

(the,263)
(of,182)
(and,119)
(that,91)
(in,80)
(to,79)
(be,68)
(ye,59)
(as,52)
(unto,49)
(a,48)
....
(And,19)
(Lord,19)
(it,19)
(Jesus,18)
(also,18)
(from,18)
(have,18)


