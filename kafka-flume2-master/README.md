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

- Data ingestion into Kafka from your local dataset in a real time: https://github.com/anevsky/kafka-flume/
