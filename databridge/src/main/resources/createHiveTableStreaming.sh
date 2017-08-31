hadoop fs -mkdir $3
hive -e "create external table if not exists zeas.$1 ($2) row format delimited fields terminated by',' lines terminated by'\n' location '$3'"