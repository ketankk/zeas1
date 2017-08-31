#!/bin/bash
hadoop fs -mkdir $4cleansed
hadoop fs -mkdir $4quarantine
hadoop fs -cp $5/cleansed/part* $4cleansed
hadoop fs -cp $5/quarantine/part* $4quarantine
hadoop fs -rmr -skipTrash $5
hive -e "create external table if not exists zeas.$1 ($2) row format delimited fields terminated by',' lines terminated by'\n' location '$3'"