# hive_udf
Hive UDFT User-Agent parser

%hive

CREATE FUNCTION agent_pars_udf AS 'UserAgentUDTF2' USING JAR 'hdfs://sandbox.hortonworks.com:8020/tmp/alex_dev/jars/task6-1.0-SNAPSHOT-jar-with-dependencies.jar'

%hive

DESCRIBE FUNCTION EXTENDED agent_pars_udf

%hive

agent_pars_udf(user_agent) uaTable AS UA_TYPE,UA_FAMILY,OS_NAME,DEVICE
