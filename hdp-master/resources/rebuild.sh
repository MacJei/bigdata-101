#!/bin/sh
#
echo "### remove users and file caches..."
rm -rf /hadoop/yarn/local/usercache/*
rm -rf /hadoop/yarn/local/filecache/*
rm -f /root/IdeaProjects/hdp/target/simple-yarn-app-1.1.0-jar-with-dependencies.jar
#
echo "### remove old jar..."
hadoop fs -rm -skipTrash /apps/simple/simple-yarn-app-1.1.0-jar-with-dependencies.jar
#
echo "### git update..."
git --git-dir=/root/IdeaProjects/hdp/.git fetch origin
#
echo "### build project..."
/usr/java/jdk1.8.0_102/bin/java -Dmaven.home=/usr/lib/idea-IC-162.1628.40/plugins/maven/lib/maven3 -Dclassworlds.conf=/usr/lib/idea-IC-162.1628.40/plugins/maven/lib/maven3/bin/m2.conf -Didea.launcher.port=7533 -Didea.launcher.bin.path=/usr/lib/idea-IC-162.1628.40/bin -Dfile.encoding=UTF-8 -classpath /usr/lib/idea-IC-162.1628.40/plugins/maven/lib/maven3/boot/plexus-classworlds-2.4.jar:/usr/lib/idea-IC-162.1628.40/lib/idea_rt.jar com.intellij.rt.execution.application.AppMain org.codehaus.classworlds.Launcher -Didea.version=2016.2.2 -f /root/IdeaProjects/hdp/pom.xml package
#
echo "### copy new jar to hdfs..."
/usr/hdp/2.4.0.0-169/hadoop/bin/hadoop fs -copyFromLocal /root/IdeaProjects/hdp/target/simple-yarn-app-1.1.0-jar-with-dependencies.jar /apps/simple/simple-yarn-app-1.1.0-jar-with-dependencies.jar
#
echo "### run app"
yarn jar /root/IdeaProjects/hdp/target/simple-yarn-app-1.1.0-jar-with-dependencies.jar com.hortonworks.simpleyarnapp.Client /tmp/alex_dev/user.profile.tags.us.txt 4 hdfs://sandbox.hortonworks.com:8020/apps/simple/simple-yarn-app-1.1.0-jar-with-dependencies.jar
#