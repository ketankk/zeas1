#!/bin/bash
## $1 - HDFS path from where to load data
## $2 - Table name to load data into.

hive -e "LOAD  DATA  INPATH  '$1' INTO TABLE $2"