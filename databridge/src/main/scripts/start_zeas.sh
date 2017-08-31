<<<<<<< HEAD
#!/bin/bash

###########
# CDH
###############
HADOOP_HOME=/usr/hdp/current/hadoop-client/
HADOOP_SHARE=/usr/hdp/2.4.2.0-258/hadoop/lib/
HIVE_HOME=/usr/hdp/2.4.2.0-258/hive

########
# HDP_VERSION}
#############

#HADOOP_HOME=/usr/hdp/2.2.0.0-2041/hadoop
#HIVE_HOME=/usr/hdp/2.2.0.0-2041/hive
#HADOOP_SHARE=/usr/hdp/2.2.0.0-2041

echo -e '1\x01foo' > /tmp/test_hive_server.txt
echo -e '2\x01bar' >> /tmp/test_hive_server.txt

#HADOOP_CORE=`ls /usr/lib/hadoop-1.1.2/hadoop-core-*.jar`

CLASSPATH=../Config/*:/tmp/json-simple-1.1.jar:$HIVE_HOME/conf
for jar_file_name in ${HIVE_HOME}/lib/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done
for jar_file_name in ${HADOOP_SHARE}/hadoop/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_HOME}/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_HOME}/lib/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_HOME}/client/*.jar
=======
#!/bin/bash

###########
# CDH
###############
HADOOP_HOME=/usr/hdp/current/hadoop-client/
HADOOP_SHARE=/usr/hdp/2.4.2.0-258/hadoop/lib/
HIVE_HOME=/usr/hdp/2.4.2.0-258/hive


########
# HDP_VERSION}
#############

#HADOOP_HOME=/usr/hdp/2.2.0.0-2041/hadoop
#HIVE_HOME=/usr/hdp/2.2.0.0-2041/hive
#HADOOP_SHARE=/usr/hdp/2.2.0.0-2041

echo -e '1\x01foo' > /tmp/test_hive_server.txt
echo -e '2\x01bar' >> /tmp/test_hive_server.txt

#HADOOP_CORE=`ls /usr/lib/hadoop-1.1.2/hadoop-core-*.jar`

CLASSPATH=../Config/*:/tmp/json-simple-1.1.jar:$HIVE_HOME/conf
for jar_file_name in ${HIVE_HOME}/lib/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done
for jar_file_name in ${HADOOP_SHARE}/hadoop/*.jar 					 
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_HOME}/*.jar								  
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_HOME}/lib/*.jar 								 
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_HOME}/client/*.jar					 
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_SHARE}/hadoop/lib/*.jar			 
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_SHARE}/hadoop-hdfs/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_SHARE}/hadoop-hdfs/lib/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_SHARE}/hadoop-yarn/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_SHARE}/hadoop-yarn/lib/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_SHARE}/hadoop-mapreduce/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done

for jar_file_name in ${HADOOP_SHARE}/hadoop-mapreduce/lib/*.jar
do
CLASSPATH=$CLASSPATH:$jar_file_name
done
CLASSPATH=$CLASSPATH:../databridge-0.0.1-SNAPSHOT.jar
echo $CLASSPATH


java -cp $CLASSPATH com.taphius.databridge.runner.MainApp ""


>>>>>>> 416a49c02fe9723a5a67d5a10b61656b99ae906a
