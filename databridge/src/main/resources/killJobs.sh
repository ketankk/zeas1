#!/bin/sh
val1 = $1

#runningJob = `oozie jobs -oozie http://10.6.185.142:11000/oozie -jobtype coordinator | grep -i RUNNING | grep -i "$val1" | awk '{print$1}'` >> kill.txt
runningJob=($(oozie jobs -oozie http://10.6.185.142:11000/oozie -jobtype coordinator | grep -i RUNNING | grep -i $1 | awk '{print$1}'))
#echo "${runningJob[@]}"
for i in "${runningJob[@]}"
do
   oozie job -oozie http://10.6.184.229:11000/oozie/ -kill $i
done

