echo "starting sqoop import"
hadoop fs -mkdir $5
if [ -z "$3" ]; then
sqoop import \
--connect $1 \
--username $2 \
--table $4 \
--m 1 \
--target-dir $5 \
--append
else
sqoop import \
--connect $1 \
--username $2 \
--password $3 \
--table $4 \
--m 1 \
--target-dir $5 \
--append
fi


