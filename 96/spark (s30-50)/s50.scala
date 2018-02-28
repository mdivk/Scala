Problem Scenario 50 : You have been given below code snippet (calculating an average score), with intermediate output. 

type Scorecollector= (Int, Double) 
type Personscores = (String, (Int, Double)) 

val initialscores = Array((“Fred”, 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0)) 
val wilmaAndFredScores = sc.parallelize(initialScores).cache()
val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scorecombiner, scoreMerger) 
val averagingFunction = (personscore: Personscores) => { 
val (name, (numberscores, totalScore)) = personScore (name, totalScore / numberscores) 
val averagescores = scores.collectAsMap().map(averagingFunction) 

Expected output : averagescores: scala.conection.Map[string,Double] = Map(Fred-> 91.33333333333333, Wilma-> 95.33333333333333) 
Define all three required function , which are input tor combineByKey method. e.g. (createScoreCombiner, scorecombiner, scoreMerger).And help us producing required results 
==================================================================================
Solution : 

val createScoreCombiner = (score: Double) => (1, score) 
val scorecombiner = (collector: Scorecollector, score: Double) => { 
val (numberscores, totalScore) = collector 
(numberscores + 1, totalScore + score) 
}
val scoreMerger= (collector1: Scorecollector, collector2: Scorecollector)=> { 
val (numScores1, totalScore1) = collector1
val (numScores2, totalScore2) = collector2 
(numScores1 + numScores2, totalScore1 + totalScore2) 
}
Description : 

The createScoreCombiner takes a double value and returns a tuple of (Int, Double) 

The scoreCombiner function takes a Scorecollector which is a type alias for a tuple of (Int,Double). We alias the values ot the tuple to numberScores and totalScore
(sacraficing a one-liner for readablility). We increment the number of scores by one and add the current score to the total scores received so far
The scoreMerger function takes two Scorecollectors adds the total number of scores and the total scores together returned in a new tuple. 

We then call the combineByKey tunction passing our previously defined functions. 

We take the resulting RDD, scores, and call the collectAsMap function to get our results in the form of(name,(numberScores,totalScore)). 

To get our final result we call the map function on the scores RDD passing in the averagingFunction which simply calculates the average score and returns a tuple-of(name,averageScore) 

Calculating an average is a litte trickier compared to doing a count for the simple fact that counting is associative and commutative, we just sum all values for each partition and
sum the partition values. But with averages, it is not that simple, an average of averages is not the same as taking an average across all numbes.But we can collect the total 
number scores and total score per partition then divide the total overall score by the number of scores. 
