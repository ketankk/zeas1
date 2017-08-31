Following steps will guide you on installing the Databridge Module

1) Run maven assembly goal to create a deployable .zip file from location where databridge source is checked out.
	command - mvn assembly:assembly
	You will find databridge.XXX.zip under /target folder

2) Copy the .zip deployable to machine on which it needs to be installed. (Currently supports Unix)
3) Unzip the databridge.zip file.  Command - unzip databridge.XXX.zip
4) Change directory to ../databridge.XXX/Config, here user needs to set few configuration details in "config.properties" file

	#############################################	
	DRIVER=com.mysql.jdbc.Driver
	DB_URL=jdbc:mysql://localhost/taphius  (Point to MySQL installation)
	USERNAME=changeme
	PASSWD=changeme

	##HDFS Installation Details.
	HDFS_FQDN = hdfs://hadooplab.bigdataleap.com  (Hadoop FQDN)
	FLUME_BIN = /home/hadoop/lab/software/apache-flume-1.4.0/bin/ (Path to Flume installation /bin directory)
	
	#############################################

5) Change directory to ../databridge.XXX/scripts , run command "chmod 777 start-databridge.sh"

6) Starting off the databridge process - running start-databridge.sh will take care of copying dependencies and creating required directory structure and finally kicking of databridge process.
	command ~ ./start-databridge.sh 

If you see below log trace on the screen this confirms that process has started off successfully. 
		
		2014-11-26 11:13:01 DEBUG DefaultListableBeanFactory:245 - Returning cached instance of singleton bean 'org.springframework.context.annotation.internalScheduledAnnotationProcessor'
		2014-11-26 11:13:01 INFO  MainApp:16 - Application context loaded successfully.



