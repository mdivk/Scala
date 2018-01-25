flatMap

val l = List("Hello", "How are you doing", "Let us perform word count", "As part of the word count program", "we will see how many times each word repeat")
val l_rdd = sc.parallelize(l)
val l_map = l_rdd.map(ele => ele.split(" "))
val l_flatMap = l_rdd.flatMap(ele => ele.split(" "))
val wordcount = l_flatMap.map(word => (word, "")).countByKey

                                                  
                                                  