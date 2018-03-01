# TDT4305: Big Data Architecture

## About the Project

### Phase 1: Exploratory Analysis of Twitter Dataset

**Objective:** Use Apache Spark for Big Data analysis.
**Use case:** By using TSV file with ~2.7M localized tweets; explore the tweet data, perform geographical analysis of tweet locations, analyze tweet times, perform textual analysis on tweet texts, etc. 

### Phase 2: TBD

**Objective:** TBD...
**Use case:** TBD..

## Installation and Prerequisite

All installation steps are for Windows 10. If you have another OS, these steps might not work for you.

### Java
 - Download and install [Java JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (recommend [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)) to e.g. location C:\Java\jdk.
 - Create a new environment variable (JAVA_HOME) and set the path to the installed location for jdk.
 - Update the enviromental variable "Path" to point to %JAVA_HOME%\bin.
 - Make sure that everything works by using the commands "java -v" and "javac -v" in cmd.

### Python
 - Download and install [Python 2.7](https://www.python.org/downloads/release/python-2714/) (add Python to Path during the installation).
 - Make sure that everything works by using the command "python --version" in cmd (if you have other versions of Python already installed, use the command "py -2.7 --version").

### Hadoop (not necessary)
 - Download the binary release of [Hadoop](http://hadoop.apache.org/releases.html) (e.g. [Hadoop 3.0.0](http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.0.0/hadoop-3.0.0.tar.gz)).
 - To make sure the files are extracted successfully, we recommend using [7-zip](http://www.7-zip.org/) (in our case, we had to extract the files twice).
 - After the files has been extracted, move and rename the folder to e.g. C:\hadoop-3.0.0.
 - Download the files "hadoop.dll" and "winutils.exe" from [Github](https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin) and place them in the "hadoop-3.0.0/bin"-folder (use at your own risk).
 - Create a new environmental variable (HADOOP_HOME) and set the path to the previous extracted and moved hadoop-3.0.0 folder.
 - Update the environmental variable "Path" to point to %HADOOP_HOME%\bin.
 - Make sure that everything works by using the command "hadoop" in cmd.

### Apache Spark
 - Download the most recent release ("Pre-built for Hadoop 2.7 and later") of [Apache Spark](http://spark.apache.org/downloads.html).
 - To make sure the files are extracted successfully, we recommend using [7-zip](http://www.7-zip.org/) (in our case, we had to extract the files twice).
 - After the files has been extracted, move and rename the folder to e.g. C:\spark.
 - Next [winutils.exe](http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe) has to be downloaded and placed in the spark\bin-folder (use at your own risk).
 - Create a new environmental variable (SPARK_HOME) and set the path to the previously extracted, moved and renamed "spark"-folder.
 - Update the environmental variable "Path" to point to %SPARK_HOME%\bin.
 - Within the spark folder, create a new folder named "tmp" and another folder within tmp named "hive" (so that the structure is: spark/tmp/hive).
 - Using cmd, go to the "spark\bin"-folder and run the command: "winutils.exe chmod -R 777 ..\tmp\hive".
 - Make sure that everything works by using the command "pyspark.cmd" from the "spark\bin"-folder in cmd.