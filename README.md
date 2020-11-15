## Big Data Demo

Big Data Demo (Spark 3 / Scala / Java 8)  

### System Requirements
In general, Spark is going to be both CPU and memory bound.  This is both true for running the job via spark-submit, 
or running Scala unit tests.   Also, the Scala compiler is a beast in terms of CPU and memory hog.  
The Intellij IDEA As such, the more cores and more memory you have, the better off your development experience will be:
Ideal:  **64GB RAM,  8+ cores / 16T, with SSD for storage**
Medium: **32GB RAM,  4 cores / 8T, SSD**
Minimum:   16GB RAM, 4 cores / 4T
Masochist:   8GB RAM, 4 cores  <- this is highly unlikely to run or compile
 You need the following runtimes or libraries.  Note that other than Visual Studio Professional, all other parts do not require explicit installation execution.  i.e. even if you don't have admin on your local machine, you should be able to have these installed.
- Some distribution of Java 8 SDK runtime.  If you use Oracle's distribution, make sure your version is *less* than java version "1.8.0_202" due to changes in license agreements from Oracle
- Intellij IDEA Community Edition (You do **NOT** need Enterprise edition)
- Spark 3.0.1 binary distribution (compiled for Hadoop 2.7 - try 3.2 at your own peril)
- Hadoop Winutils binary (optional, helps suppress some warnings)
access) 
- *Optional*   Zeppelin 0.9 preview (for interactive testing of Spark using Scala - not stable, but 0.8.2 doesn't support Spark 3)
- *Optional*   Install Jupyter (TBD) 
Set Environment Variable 

### Installation

Follow instructions on this page (but please see Java 8 SDK installation instructions below first, or you may be violating Oracle licensing agreements)
https://docs.microsoft.com/en-us/dotnet/spark/tutorials/get-started

Alternative installation below (very similar to Microsoft's instructions, with some slight version differences)

Java 8 SDK (alternate installation for JDK - for Work, do not install version above build 202 for Java 1.8)
1. Install Java 8 SDK - I recommend using the attached zip in the slack channel: #data-platform-spark-dotnet   https://brighthealthplan.slack.com/files/U015DTD4LP9/F01833DRQ1X/java8sdk.zip
1. unzip java8sdk.zip into folder of your choice:  e.g.  **c:\jdk8**
1. add SYSTEM ENV variable **JAVA_HOME=c:\jdk8**  (or full path to where you installed/unzipped this)
1. add **c:\jdk8\bin**  to your PATH
1. To test your installation, open new cmd prompt,
    run: **java -version**   
    You should see JVM runtime information

### Spark 3.0.1
1.  Goto **https://spark.apache.org/downloads.html**
1.  Choose Pre-built for Apache Hadoop 2.7
1.  Download **spark-3.0.1-bin-hadoop2.7.tgz**   https://www.apache.org/dyn/closer.lua/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
1.  untar (using cmdline tar xvf) somewhere on your drive.  It will create a folder called: **spark-2.4.6-bin-hadoop2.7\**
1.  you can rename this folder to be something shorter if you wish
1.  add SYSTEM ENV variable **SPARK_HOME=** directory where you put this
1.  add SYSTEM ENV variable **HADOOP_HOME=** directory where you put this
1.  add **(base folder)\spark-3.0.1-bin-hadoop2.7\bin** to your PATH
1.  To test your installation, open new cmd prompt, 
1.  modify your conf/spark-defaults.conf to include **spark.driver.memory 12g**
   run: **spark-shell**   
   You should get a spark shell interpreter running

### Hadoop Winutils binaries

1.  I recommend using the attached zip in the slack channel: https://brighthealthplan.slack.com/files/U015DTD4LP9/F017QEMB6D8/hadoop-winutils.zip
1.  unzip to c:\hadoop (or folder of your choice)
1.  copy the winutils.exe from this bin folder to your **spark\bin**  directory

### SQL Server Support for Spark (optional)

1.  Download https://go.microsoft.com/fwlink/?linkid=2122433
1.  unzip it somewhere
1.  Find **mssql-jdbc-8.2.2.jre8.jar**, and ut that file into your SPARK_HOME/jars directory

### Zeppelin installation

1.  On native windows, currently Zeppelin-0.9.0-preview doesn't work quite right. :-()
2.  Goto https://zeppelin.apache.org/download.html
1.  Follow instructions on website for download, unzip, and run.
1.  This is a web application, so once you run it, use your Chrome Browser (doesn't work well with others) to hit http://localhost:8080/
1.  It should pick up your SPARK_HOME ENV variable.  If not, you'll need to configure Zeppelin to use your spark distro, otherwise it won't have the Azure and  SQL Server support directly

### Build

Either use Intellij IDEA with scala support (import maven project)

OR

install maven 3.6+  (download zip from: https://maven.apache.org/ref/3.6.3/ , put bin/ folder in PATH)
mvn package

### Build and run

This with do a mvn package which:
1. compiles
2. runs unit test
3. builds jar including 3rd party dependencies in target/
4. clean out output directory, and
5. runs spark submit on your built jar

### Windows
buildrun.bat from demo-loader module's root

### Debug
Use Intellij IDEA's Debugger
Make sure to check the option in (Run Configurations):

**"Include Dependencies with Provided Scope"**

The maven build pom.xml has all Scala and Spark dependencies declared as "provided", which makes resulting binary super lean and small, but this means when you debug, you have to tell the IDE to included it from elsewhere.

### More Resources
- https://spark.apache.org/docs/latest/quick-start.html
- https://databricks.com/spark/about
- https://github.com/dotnet/spark/issues/529
- Many many other sources of both good, and not so good Spark information, happy to chat about this


