echo $1
echo $2
echo $3
echo $4
hive -e "create view if not exists zeas.$1 as select $2 from (select $2,ROW_NUMBER() OVER(partition by $3 ORDER BY ingestiontime DESC) as r from zeas.$4) s where r=1"