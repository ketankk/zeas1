#!/bin/bash
if [ "$7" = "sparkC" ]
then
	spark-submit --master yarn-cluster  /home/zeas/zeas/Config/Streaming/library/ConsumerDriver.jar --broker $1 --port $2 --group $3 --topic $4 --user $5 --duration $6 $7
elif [ "$7" = "flinkC" ]
then
	echo "Flink"
fi

