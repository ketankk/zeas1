::Script to automate the build and deployment of war on server

::1.Build the WAR
::2.ShutDown tomcat
::3.Rename existing war
::4.Delete ZDP-WEB folder
::5.Copy war from local to linux
::6.Start server
echo ZEAS

::First build the new war
:: %1 is parameter for build type Profile, Local/PreProd/AWS


start /wait build.bat %1
::Building Zeas with dev-settings and mode is Local/PreProd/AWS
::mvn -f %ZEAS_HOME%/ZDP-Master/pom.xml clean install -s %ZEAS_HOME%/ZDP-Master/dev-settings.xml -P %1

::Run script on server to stop server, for that connect to server
::%2 is instance number 90/91

if "%1%" == "Local" (
set Host=10.6.185.142


) ELSE (
if "%1%"=="PreProd" (
set Host=10.6.185.15

)
)


if "%2%" == "90" (
set Tomcat=tomcat7
) ELSE (
if "%2%"=="91" (
set Tomcat=tomcat7_inst2

)
)

set /P password= Please enter password for user zeas:
::echo %Host%
::echo %password%

::this will create a file, which will stop the server and rename old war file
(
echo sh /home/zeas/zeas/bin/zeas %2% stop
echo sh /home/zeas/zeas/bin/rename.sh %Tomcat%
)>stopRename


::connect through putty and run that stop and rename scripts
start  putty.exe -ssh %HOST% -l zeas -pw %password% -m stopRename

::sh /home/zeas/zeas/bin/zeas %2 stop
::setting timeout for copying
timeout /T 3
::Copy new war file to server %Tomcat% is tomcat7 for 90 port tomcat7_inst2 for 91 port
start /wait copywar.bat %Tomcat%


(
echo sh /home/zeas/zeas/bin/zeas %2% start
)>start
::timeout /T 5

::connect through putty and run that stop and rename scripts
start  putty.exe -ssh %HOST% -l zeas -pw %password% -m start








