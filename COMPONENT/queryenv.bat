@echo off
set WORKING_DIR=%~dp0
setx CLASSPATH "%WORKING_DIR%;%WORKING_DIR%\classes;%WORKING_DIR%\lib\java_cup;%WORKING_DIR%\lib\JLex;"
setx COMPONENT "%WORKING_DIR%"
echo "Query environment setup successfully"