Pre-Requisites
==============

Access
------
1. Get access to ZEAS codebase (GitHub)
2. if you are working from ITC LAN, raise ticket to get your hostname in different internet policy (should say please add me into internet policy same as <one_of_existing_team_member>
3. Download and configure git shell / plug-in for windows (https://git-scm.com/download/win)

Software Required
1. maven (latest)
2. Git Bash for windows (https://git-scm.com/download/win)
3. JDK 1.7+(including JRE)
4. Ecplise (Mars+)
5. Tomcat (1.7+)


Codebase Pull
-------------
1. GitHub Configuration Change:
git config --global user.name "Pradeep Kumar"
$ git config --global user.email "Pradeep.Kumar@itcinfotech.com"
$ git config --global http.proxy "19726@10.6.13.210:8080"
$ git config --global https.proxy "19726@10.6.13.210:8080"
$ git config --global http.sslverify "false"
$ git config --global https.sslverify "false"

2. Code Pull
	a. Open Git shell / command line. It should take you to the path where ".git" file exists
	b. Go to directory where you need to keep the code (cd <target_path>)
	c. execute command "git init"
3. run pull command "git clone https://github.com/itcbdlob/zdp.git"
4. go to code home "cd zdp"
5. execute "git init" to initialize git for this project
6. Checkout correct branch for development "git checkout Wave3.0" (Note: branch might change in future, please discuss with team)

Codebase setup
--------------
1. Maven "setting.xml" changes :
Add following Proxy XML element in Maven settings.xml (would be there inside ".m2" directory)

<proxy>
      <id>optional</id>
      <active>true</active>
      <protocol>http</protocol>
      <username><proxyuser></username>
      <password><proxypass></password>      
      <host>10.6.13.210</host>
      <port>8080</port>
      <nonProxyHosts>maven</nonProxyHosts>
</proxy>

For SSL issue, 
a. add below XML element under <settings> tag

<activeProfiles>   
    <activeProfile>securecentral</activeProfile>
 </activeProfiles>

b. Add profile element under <profiles>

<profile>
      <id>securecentral</id>      
      <repositories>
        <repository>
          <id>central</id>
          <url>http://repo1.maven.org/maven2</url>
          <releases>
            <enabled>true</enabled>
          </releases>
        </repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository>
          <id>central</id>
          <url>http://repo1.maven.org/maven2</url>
          <releases>
            <enabled>true</enabled>
          </releases>
        </pluginRepository>
      </pluginRepositories>
    </profile>
                
2. Go to ZDP-Master folder and execute "mvn clean"

Eclipse Setup
-------------
1. Open Eclipse (Mars+) with new workspace path
2. File -> Import -> Maven -> Existing Maven Projects
    a. provide path where ZEAS code was cloned (till ZDP-Master)
	b. Finish
	

workspace build
---------------
1. create folder "zeas/Config" inside %USER_HOME% (C:\Users\17038\zeas\Config)
2. Copy "config.properties" from "resources" of project "databridge" to above new directlry
3. change "DB_URL" and corresponding USERNAME, PASSWD to the ones available now
4. Go to "ZDP-Master" project path
5. open "dev-settings.xml"
6. Change db-url, db.username, db.password as applicable
7. run command "mvn clean install -s dev-settings.xml -P Local" from this project "ZDP-Master"
8. Reload the ZEAS projct from eclipse

Congratulations! you are good to go.


Troubleshooting
===============
1. pom.xml error
"Description	Resource	Path	Location	Type
Failure to transfer org.apache.maven.plugins:maven-resources-plugin:pom:2.6 from http://repo1.maven.org/maven2 was cached in the local repository, resolution will not be reattempted until the update interval of central has elapsed or updates are forced. Original error: Could not transfer artifact org.apache.maven.plugins:maven-resources-plugin:pom:2.6 from/to central (http://repo1.maven.org/maven2): Access denied to http://repo1.maven.org/maven2/org/apache/maven/plugins/maven-resources-plugin/2.6/maven-resources-plugin-2.6.pom. Error code 407, Proxy Authorization Required	pom.xml	/ZDP-Web	line 1	Maven Configuration Problem
"

solution:
put following dependency in the ZDP-Master pom.xml inside <build> -> <plugins> node
<plugin>
	<artifactId>maven-resources-plugin</artifactId>
	<version>2.7</version>
</plugin>

2. pom.xml error
Missing artifact jdk.tools:jdk.tools:jar:xxxxxxxxxxx

solution:
<dependency>
	<groupId>jdk.tools</groupId>
	<artifactId>jdk.tools</artifactId>
	<version>1.7.0_79</version>
	<scope>system</scope>
	<systemPath>${JAVA_HOME}/lib/tools.jar</systemPath>
</dependency>


Note : below changes are for HDP Env,please remove value if ZEAS is not using HDP
##Path to Hadoop installation directory
HADOOP_HOME=/usr/hdp/current/hadoop-client/
HADOOP_CONF=/etc/hadoop/2.4.2.0-258/0/
