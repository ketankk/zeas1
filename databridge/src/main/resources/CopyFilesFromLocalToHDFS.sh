#!/bin/bash
hadoop fs -rmr $2$3
hadoop fs -mkdir -p $2
hadoop fs -copyFromLocal $1 $2
##Create project output directory
hadoop fs -mkdir -p $4
hadoop fs -chgrp yarn $4
hadoop fs -chmod 777 $4