#!/usr/bin/env bash

cd /root/projects/
git clone https://github.com/anevsky/spark.git
cd /root/projects/spark

git pull origin master

export MAVEN_OPTS="-Xms1g -Xmx2g"
mvn package

###
export SPARK_MAJOR_VERSION=2
/usr/hdp/current/spark2-client/bin/spark-submit \
--class "com.epam.bigdata.spark.KeywordGrabber" \
--master yarn \
/root/projects/spark/target/task8-1.0.0-jar-with-dependencies.jar \
logs_orc \
/tmp/alex_dev/user.profile.tags.us.tags.txt \
/tmp/alex_dev/city.us.txt
