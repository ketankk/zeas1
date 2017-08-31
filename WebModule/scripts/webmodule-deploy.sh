#!/bin/bash
############
### Environment Variable update
############
if [ -z "$1" ] 
then
  echo "Usage: webmodule-deploy.sh webserverpath"
  exit -1
fi

if [ -z "$2" ] 
then
  echo "Usage: webmodule-deploy.sh webserverpath"
  exit -1
fi

if [ -z "$3" ] 
then
  echo "Usage: webmodule-deploy.sh webserverpath"
  exit -1
fi

if [ -z "$4" ] 
then
  echo "Usage: webmodule-deploy.sh webserverpath"
  exit -1
fi


webServer=$1;
mysqlUrl=$2;
username=$3;
passwd=$4;

warDir=$(cd ..; pwd)
cd $webServer/webapps
rm -rf mkdir $webServer/webapps/WebModule*
mkdir $webServer/webapps/WebModule
cd $webServer/webapps/WebModule
jar -xvf $warDir/Web*.war

config_dir=$(cd $webServer/webapps/WebModule/WEB-INF/classes; pwd) 

echo "Updating SQL connection details into Config file..$config_dir"

cd $config_dir

sed "s/localhost/$mysqlUrl/g" config.properties > temp
mv temp config.properties;

sed "s/root/$username/g" config.properties > temp
mv temp config.properties;

sed "s/changeme/$passwd/g" config.properties > temp
mv temp config.properties;

cat $config_dir/config.properties

echo "Restarting webserver.."

cd $webServer/bin

./shutdown.sh
./startup.sh

