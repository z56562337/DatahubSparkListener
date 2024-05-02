package com.garmin.dsii.datahub.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InitHelper {

  def generateSparkSession(envConfig: EnvConfig, testName: String) = {
    val sparkConf = new SparkConf()
      .setMaster(envConfig.sparkConf.master)
      .setAppName(s"${envConfig.sparkConf.appName}-$testName")

    if (envConfig.hdfsConf.haEnable) {
      val namespaceConfig = envConfig.config.getConfig("hdfs.namespace")
      val namespace = namespaceConfig.getString("nameservices")
      val node1 = namespaceConfig.getConfig("namenode1").getString("name")
      val node1Addr = namespaceConfig.getConfig("namenode1").getString("address")
      val node2 = namespaceConfig.getConfig("namenode2").getString("name")
      val node2Addr = namespaceConfig.getConfig("namenode2").getString("address")
      val failoverProxy = namespaceConfig.getString("failoverProxyProvider")

      sparkConf
        .set("hive.metastore.uris", envConfig.hiveConf.metastore)
        .set("fs.defaultFS", envConfig.hdfsConf.host)
        .set("dfs.nameservices", namespace)
        .set(s"dfs.ha.namenodes.${namespace}", s"${node1},${node2}")
        .set(s"dfs.namenode.rpc-address.${namespace}.${node1}", node1Addr)
        .set(s"dfs.namenode.rpc-address.${namespace}.${node2}", node2Addr)
        .set(s"dfs.client.failover.proxy.provider.${namespace}", failoverProxy)
        .set("spark.extraListeners", "com.garmin.dsii.datahub.listener.DatahubSparkListener")
        .set("spark.garmin.datahub.rest.server", "http://linxta-pesmack02.garmin.com:8080")
        .set("spark.hdfs.ingestion.input.path", "hdfs://linxta-pecdp01/data/test/")
        .set("spark.hdfs.ingestion.output.path", "hdfs://linxta-pecdp01/data/testOutput/")
    }

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // Turn on flag for Hive Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark
  }
}