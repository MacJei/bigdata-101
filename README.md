# Big Data ramp-up

# hdp simple-yarn-app

Simple YARN application to count top words in provided URL list from .txt file

Usage

- See /resources/rebuild.sh 
- Or mvn package and run with command like following:
- yarn jar /root/IdeaProjects/hdp/target/simple-yarn-app-1.1.0-jar-with-dependencies.jar com.hortonworks.simpleyarnapp.Client /tmp/alex_dev/user.profile.tags.us.txt 4 hdfs://sandbox.hortonworks.com:8020/apps/simple/simple-yarn-app-1.1.0-jar-with-dependencies.jar

# mr-count
MapReduce Counting

- Load data to HDFS

Run as following

- mvn package
- yarn jar /root/IdeaProjects/mr-count/target/task3-1.0.0-jar-with-dependencies.jar com.epam.bigdata.task3.jobs.MRTagsJob /tmp/alex_dev/user.profile.tags.us.txt.out /tmp/alex_dev/hw3/mr1/out [/tmp/alex_dev/ignore-words-lines]
- yarn jar /root/IdeaProjects/mr-count/target/task3-1.0.0-jar-with-dependencies.jar com.epam.bigdata.task3.jobs.MRVisitsSpendsJob /tmp/alex_dev/dataset /tmp/alex_dev/hw3/mr2/out

How to read to text Snappy file in HDFS
- hadoop fs -libjars /root/IdeaProjects/mr-count/target/task3-1.0.0-jar-with-dependencies.jar -text /tmp/alex_dev/hw3/out/part-r-00000 | head

Notes

- DistributedCache for optional input for stop/bad words in MRTagsJob.
- Output as Sequence file with Snappy compression for output in MRVisitsSpendsJob.
- Custom implementation of WritableComparable MPVisitSpendDTO for MRVisitsSpendsJob.
- UserAgent parser https://github.com/HaraldWalker/user-agent-utils.
- Unit tests.
- You can run jobs and tests like simple java applications: public static void main(String[] args).

# mr-count2

MapReduce Secondary Sort

Load data to HDFS

Build
- mvn package

Run
- Usage: MRPinSecondarySortDriver <in> <out> <numReduceTasks>
- yarn jar /root/IdeaProjects/mr-count2/target/task4-1.0.0-jar-with-dependencies.jar com.epam.bigdata.task4.mr.MRPinSecondarySortDriver /tmp/alex_dev/dataset /tmp/alex_dev/hw4/out 4


Notes
- Sort by iPinyouId & timestamp;
- Using SnappyCodec for Map output;
- Using counter for getting [Insights] [Max Impression]

# hive_udf
Hive UDFT User-Agent parser

%hive

CREATE FUNCTION agent_pars_udf AS 'UserAgentUDTF2' USING JAR 'hdfs://sandbox.hortonworks.com:8020/tmp/alex_dev/jars/task6-1.0-SNAPSHOT-jar-with-dependencies.jar'

%hive

DESCRIBE FUNCTION EXTENDED agent_pars_udf

%hive

agent_pars_udf(user_agent) uaTable AS UA_TYPE,UA_FAMILY,OS_NAME,DEVICE

# kafka-flume

- Kafka Producer: Data ingestion into Kafka from your local dataset in a real time
- Kafka Consumer for Topic

Please also see: 

- Data flow for DWH stream (Kafka-Flume-HDFS/HIVE) https://github.com/anevsky/bigdata-101/tree/master/kafka-flume2-master

# kafka-flume2
Flume Custom Interceptor

Data flow for DWH stream (Kafka-Flume-HDFS/HIVE):

1) Kafka -> Flume Agent (kafka-source)

2) Flume Agent (kafka-source) -> Flume Agent (Interceptor)
- Interceptor need to add real tags based on tag Id (first it will take tags from your file, late will be replaced with HBase/Cassandra)

3) Flume Agent (Interceptor) -> Flume Agent (Channel Selector)
- Select channel based on added tags (If tags has been added – Channel1, if not – Channel2)

3) Flume Agent (Channel1) -> Flume Agent (HDFS/Hive Sink)
- Data for DWH (need to be partitioned), use it in Segment Report

4) Flume Agent (Channel2) -> Flume Agent (HDFS Sink)
- Tags for future Crawling (Think about storing your MR crawler results into fast NoSQL storage).

Please also see:

- Data ingestion into Kafka from your local dataset in a real time: https://github.com/anevsky/bigdata-101/tree/master/kafka-flume-master

# spark
Big Data: Spark + Facebook API

# spark2
Big Data: Kafka + Spark Streaming + HBase + Apache Phoenix + Hive + Apache Kylin + Zeppelin

- Data ingestion into Kafka from your local dataset in a real time
https://github.com/anevsky/bigdata-101/tree/master/kafka-flume-master
- Spark Streaming from Kafka into HBase - DWH backup storage & Session hot row cache)
- Using Apache Phoenix (>= 4.7) to wrap tables in HBase and create corresponding Meta representation in Hive
- Using Apache Kylin to describe and build cubes for 2 types of reports from Hive tables
- Using Kylin Interpreter for Zeppeling 
