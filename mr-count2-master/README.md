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