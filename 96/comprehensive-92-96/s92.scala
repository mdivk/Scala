Problem Scenario 92 : You have been given a spark scala application, which is bundled in jar named hadoopexam.jar . Your application class nam 

You want that while submitting your application should launch a driver on one ot the cluster node. Please complete the following command:

spark-submit hadoopexam.jar --master yarn 

SSPARK_HOME/lib/hadoopexam.jar 10 

Solution 
XXX : —class com.hadoopexam.MyTask 
yyy : --deploy-mode cluster 
