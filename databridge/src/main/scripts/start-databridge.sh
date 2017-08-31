#!/bin/bash
############
### Environment Variable update
#########
mkdir -p $HOME/taphius/databridge
DATABRIDGE=$HOME/taphius/databridge

##Directory to store FLUME ingestion configurations.
mkdir -p $DATABRIDGE/flume/conf

DATABRIDGE_CONF=$DATABRIDGE/flume/conf/

cp ../Config/*.conf $DATABRIDGE_CONF

##Directory holds all dependency jars
mkdir -p $DATABRIDGE/lib

cp ../lib/* $DATABRIDGE/lib

CLASSPATH="${DATABRIDGE}/flume/lib/*:../Config/*"

##echo  FLUME_CONF_DIR=$DATABRIDGE_CONF >> ../Config/config.properties

java -cp "${CLASSPATH}:../databridge-0.0.1-SNAPSHOT.jar"  com.taphius.databridge.runner.MainApp $DATABRIDGE_CONF
