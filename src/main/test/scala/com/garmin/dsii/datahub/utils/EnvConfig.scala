package com.garmin.dsii.datahub.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStreamReader
import java.net.URI
import scala.collection.JavaConverters._

case class KafkaConsumerInfo(bootstrapServers: String, topics: Iterable[String], keyDeserializer: String, valueDeserializer: String, groupId: String, autoOffsetReset: String, autoCommit: java.lang.Boolean, autoCommitInterval: String)

case class SparkInfo(master: String, appName: String)

case class HdfsInfo(host: String, markerPath: String, checkPointDir: String, haEnable: Boolean, gracefulEnable: Boolean)

case class HiveInfo(metastore: String)

class EnvConfig(val config: Config) {

  val sparkConf = SparkInfo(
    config.getString("spark.master"),
    config.getString("spark.appName")
  )

  val hdfsConf = HdfsInfo(
    config.getString("hdfs.host"),
    config.getString("hdfs.markFilePath"),
    config.getString("hdfs.checkPointDir"),
    config.getBoolean("hdfs.haEnable"),
    config.getBoolean("hdfs.gracefulEnable")
  )

  val hiveConf = HiveInfo(
    config.getString("hive.metastore")
  )
}

object EnvConfig {
  def apply(hdfsHost: String, filePath: String) = {
    val config = loadConfigFromHDFS(hdfsHost, filePath)
    new EnvConfig(config)
  }

  def loadConfigFromHDFS(hdfsHost: String, filePath: String): Config = {
    val hdfs = FileSystem.get(new URI(hdfsHost), new Configuration())
    val reader = new InputStreamReader(hdfs.open(new Path(filePath)))
    val stopFlag = hdfs.exists(new Path(filePath))
    ConfigFactory.parseReader(reader)
  }
}