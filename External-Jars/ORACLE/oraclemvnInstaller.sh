#!/bin/bash
mvn install:install-file -DgroupId=oracle -DartifactId=ojdbc6 -Dversion=11.2.0.3 -Dpackaging=jar "-Dfile=ORACLE/ojdbc6.jar"