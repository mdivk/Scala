Problem Scenario 94 : You have to run your Spark application on yarn with each executor 20GB and number ot executors should be 50. Please replace XXX, YYY, ZZZ

Export Hadoop_conf_DIR=XXX
./bin/spark-submit \ 
--class com.hadoopexam.MyTask \ 
xxx \ 
--deploy-mode cluster \ # can be client tor client mode 
zzz \ 
/path/to/hadoopexam.jar \ 
1000 

Solution 

XXX : --master yarn 
YYY : —executor-memory 20G 
ZZZ : —num-executors 50 

./bin/spark-submit \
--class com.hadoopexam.MyTask \
--master yarn \
--deploy-mode cluster \
--executor-memory 20G 
--num-executors 50

# Run application locally on 8 cores
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master local[8] \
  /path/to/examples.jar \
  100

# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /path/to/examples.jar \
  1000