#Run script file to start twitter agennt
#arguments are $1=Flume Home folder; $2=Flume-agent configuration file; $3=kafkaTopic;


$1/bin/flume-ng agent --conf conf --conf-file $2 --name $3

