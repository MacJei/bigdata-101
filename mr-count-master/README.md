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