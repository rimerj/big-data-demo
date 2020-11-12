@echo off
cd ..
REM call mvn clean
call mvn package -DskipTests
cd demo-loader
call runit.bat 2> error.txt
