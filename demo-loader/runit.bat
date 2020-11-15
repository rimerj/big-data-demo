@echo off
echo "See error.txt for STDERR!"
rd /s/q .\test_output
echo "Removed .\test_output"

REM Detect name of JAR in targets
for /F %%x IN ('cmd /C dir /b target\*depend*') DO set JAR=%%x
echo "Jar NAME: %JAR%"
spark-submit --class com.bhp.dp.demo.DemoLoader target\%JAR% -target "./test_output/"
