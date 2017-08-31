#!/bin/bash
$TOMCAT_PATH=/usr/local/$1/webapps
$CURRENT_TIME=date +%Y%m%d%H%M%S
mv ZDP-Web ZDP-Web_$CURRENT_TIME
