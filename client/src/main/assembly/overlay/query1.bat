@echo off
setlocal enabledelayedexpansion

REM Get the current directory and set it as the code base
for %%F in ("%~dp0.") do set "PATH_TO_CODE_BASE=%%~fF"

REM Set the main class
set "MAIN_CLASS=ar.edu.itba.pod.tp2.client.Client1"

REM Set the JAVA_OPTS if needed
REM set "JAVA_OPTS=-Djava.rmi.server.codebase=file://!PATH_TO_CODE_BASE!\lib\jars\rmi-params-client-1.0-SNAPSHOT.jar"

REM Run the Java application
java %* -cp "lib\jars\*" %MAIN_CLASS%

endlocal