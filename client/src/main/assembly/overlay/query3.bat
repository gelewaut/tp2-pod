@echo off
setlocal

REM Set the path to the code base
set "PATH_TO_CODE_BASE=%CD%"

REM Set the main class
set "MAIN_CLASS=ar.edu.itba.pod.tp2.client.Client3"

REM Set Java options (optional)
set "JAVA_OPTS=-Djava.rmi.server.codebase=file://%PATH_TO_CODE_BASE%/lib/jars/rmi-params-client-1.0-SNAPSHOT.jar"

REM Run the Java application
java %JAVA_OPTS% -cp "lib/jars/*" %MAIN_CLASS% %*

endlocal