echo Uploading file on %1
(
echo open 10.6.185.142
echo zeas
echo Zeas@2017
echo put %ZEAS_HOME%\ZDP-Master\ZDP-Web\target\ZDP-Web.war /usr/local/%1/webapps/ZDP-Web.war
echo quit
)>login.ftp


ftp -s:login.ftp
exit