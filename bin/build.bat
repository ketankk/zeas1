::Building Zeas with dev-settings and mode is Local/PreProd/AWS
mvn -f %ZEAS_HOME%/ZDP-Master/pom.xml clean install -s ../ZDP-Master/dev-settings.xml -P %1
exit