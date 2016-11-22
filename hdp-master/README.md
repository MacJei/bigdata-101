# hdp simple-yarn-app

Simple YARN application to count top words in provided URL list from .txt file

Usage

- See /resources/rebuild.sh 
- Or mvn package and run with command like following:
- yarn jar /root/IdeaProjects/hdp/target/simple-yarn-app-1.1.0-jar-with-dependencies.jar com.hortonworks.simpleyarnapp.Client /tmp/alex_dev/user.profile.tags.us.txt 4 hdfs://sandbox.hortonworks.com:8020/apps/simple/simple-yarn-app-1.1.0-jar-with-dependencies.jar
