#!/bin/bash
##oozie job -oozie http://192.168.157.129:11000/oozie/ -config /home/hadoop/zeas/pipeline/TestingOozie/job.properties -run
oozie job $1 $2 $3
