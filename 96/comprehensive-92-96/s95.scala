Problem Scenario 95 : You have to run your Spark application on yarn with each executor Maximum heap size to be 512MB and Number ot processor cores to allocate on each executor will be 1 and your main application required three values as input arguments V1V2V3.pleasee replace XXX, YYY, ZZZ 

./bin/spark-submit —class com.hadoopexam.MyTask --master yarn-cluster —num-executors 3 —driver-memory 512m XXX YYY lib/hadoopexam.jar ZZZ 

Solution 
XXX : --executor-memory 512m 
YYY : --executor-cores 1 
zzz : V1V2V3 
Notes : spark-submit on YARN Options 
Option Description 
archives Comma-separated list ot archives to be extracted into the working directory oF each executor. The path must be globally visible inside Your cluster; see Advanced Dependency Management.
executor-cores Number ot processor cores to allocate on each executor. Alternatively, you can use the spark.executor.cores property. 

executor-memory Maximum heap size to allocate to each executor. Alternatively, you can use the spark.executor.memory property. 
num-executors Total number ot YARN containers to allocate for this application. Alternatively, you can use the spark.exécUtor.instances property. property. 
queue YARN queue to submit to. For more information, see Assigning Applications and Queries to Resource Pools. 
Detault: default. 


./bin/spark-submit \
--class com.hadoopexam.MyTask \
--master yarn \
--cluster \\
--num-executors 3 \
--executor-memory 512MB \
lib/hadoopexam.jar
