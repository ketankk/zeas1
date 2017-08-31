echo "starting sqoop import"
echo "removing hadoop directory if exists"
hadoop fs -rm -r $5
cd $SQOOP_HOME;
./bin/sqoop import \
--connect $1 \
--username $2 \
--password $3 \
--table $4 \
--columns $6 \
--m 1 \
--target-dir $5


