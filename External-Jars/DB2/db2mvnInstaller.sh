#!/bin/bash
mvn install:install-file -DgroupId=com.ibm.db2 -DartifactId=db2jcc_license_cisuz -Dversion=8.1.1 -Dpackaging=jar "-Dfile=DB2/db2jcc4.jar"