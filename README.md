# Datahub Spark Listener

## Introduction

   To integrate Spark with DataHub, we provide a lightweight Java agent that listens for Spark application and job events and pushes metadata lineage out to DataHub in real-time.

## Supported DB Type
- Hive
- JDBC
- HDFS

## Configuring in Project

<pre>
# Configuring build.sbt
libraryDependencies += "com.garmin.dsii" % "datahub-listener_2.11" % "1.0.0"
</pre>

<pre>
# Configuring DataHub spark agent jar
spark = SparkSession.builder
          .master("spark://spark-master:7077")
          .appName("test-application")
          .set("spark.extraListeners", "com.garmin.dsii.datahub.listener.DatahubSparkListener")
          .set("spark.garmin.datahub.rest.server", "http://linxta-pesmack02.garmin.com:8080")
          .set("spark.hdfs.ingestion.input.path", "hdfs://linxta-pecdp01/data/test/")
          .set("spark.hdfs.ingestion.output.path", "hdfs://linxta-pecdp01/data/test/")
          .getOrCreate()
     
# properties meaning     
spark.extraListeners: set listener object
spark.garmin.datahub.rest.server: set datahub host
spark.hdfs.ingestion.input.path: set the name you want to show as datahub hdfs entity 
                                 (for input hdfs sources, default will be full file path)
spark.hdfs.ingestion.output.path: set the name you want to show as datahub hdfs entity
                                 (for output hdfs sources, default will be full file path)
</pre>

<pre>
# debugging setting in log4j.properties
log4j.logger.com.garmin.dsii.datahub=DEBUG
</pre>

## datahub-listener 1.0.2 Release Notes
###  New Features
- Delete all task in spark pipeline at initial 

## datahub-listener 1.0.1 Release Notes
###  Bug Fix
- Handle "InsertIntoHadoopFsRelationCommand" insert hive table event
- Changed Logger debug to info

## datahub-listener 1.0.0 Release Notes
###  New Features
- supported Hive, JDBC, HDFS lineage auto generation
- supported HDFS datahub entity auto creation
- supported spark pipeline datahub entity auto creation
