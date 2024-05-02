package com.garmin.dsii.datahub.listener

import com.garmin.dsii.datahub.utils.{EnvConfig, InitHelper, RestEmitterUtil}
import com.linkedin.common.Status
import com.linkedin.common.urn.Urn
import org.apache.spark.sql.SaveMode
import org.scalatest.featurespec.AnyFeatureSpec

class DatahubSparkListenerTest extends AnyFeatureSpec {
  Feature("DatahubSparkListener") {
    Scenario("DatahubSparkListener") {

      val hdfsHost = "localhost"
      val filePath = "./src/main/test/resources/config/application.conf"
      val testName = "Test!"
      val platform = "CDP"

      System.setProperty("user.name", "hdfs")
      System.setProperty("HADOOP_USER_NAME", "hive")
      val envConfig = EnvConfig(hdfsHost, filePath)
      val spark = InitHelper.generateSparkSession(envConfig, testName)
      println(spark.sparkContext.appName)


      val ds = spark.read
        .format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", "jdbc:sqlserver://;serverName=t1-pe-support3d")
        .option("dbtable", "PESupport.dbo.OEEResultTable")
        .option("user", "pieng")
        .option("password", "Q2iT5cwHJW3FH")
        .load()

      val ds1 = spark.read
        .table("test.test_order_ext_orc1")
        .select("o_id", "order_no", "c_id")
      ds1.cache().show(1)

      val ds11 = spark.read
        .table("test.test_order_ext_txt1")
        .select("o_id", "order_no", "c_id")
      ds11.cache()

      val ds2 = spark.read
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://linxta-pesmack02:3306/datahub")
        .option("dbtable", "datahub.nozzleusage_test")
        .option("user", "datahub")
        .option("password", "datahub").load()

      val ds3 = spark.read
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://linxta-pesmack02:3306/datahub")
        .option("dbtable", "datahub.nozzleusage_test1")
        .option("user", "datahub")
        .option("password", "datahub").load()

      ds1.write.format("hive").mode(SaveMode.Append).saveAsTable("test.test_order_ext_orc2")
      ds.limit(10).write
        .format("com.microsoft.sqlserver.jdbc.spark")
        .mode(SaveMode.Overwrite)
        .option("url", "jdbc:sqlserver://;serverName=t1-pe-support3d")
        .option("dbtable", "PESupport.dbo.OEEResultTable1")
        .option("user", "pieng")
        .option("password", "Q2iT5cwHJW3FH")
        .save()

      val ds4 = ds2.union(ds3)

      ds4.write.mode(SaveMode.Append)
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://linxta-pesmack02:3306")
        .option("dbtable", "datahub.nozzleusage_test2")
        .option("user", "datahub")
        .option("password", "datahub")
        .save()

      val ds6 = ds1.union(ds11)
      ds6.write.format("hive").mode(SaveMode.Append).saveAsTable("test.test_order_ext_txt2")

      val inputFile1 = if (platform.equals("CDP")) "hdfs://linxta-pecdp01/data/test/Customers.csv" else "hdfs://linxta-pesmack01:8020/data/test/Customers.csv"
      val inputFile2 = if (platform.equals("CDP")) "hdfs://linxta-pecdp01/data/test/Order.csv" else "hdfs://linxta-pesmack01:8020/data/test/Order.csv"
      val outputFile = if (platform.equals("CDP")) "hdfs://linxta-pecdp01/data/test/out4.csv" else "hdfs://linxta-pesmack01:8020/data/test/out4.csv"

      val sampleDF1 = spark.read.option("header", "true").csv(path = inputFile1)
      val sampleDF2 = spark.read.option("header", "true").csv(path = inputFile2)
      val resultDF = sampleDF1.join(sampleDF2, "C_Id")
      resultDF.cache()

      resultDF.write.mode(SaveMode.Overwrite).option("header", "true").csv(path = outputFile)

      // you need to check result at your DataHub ui
      // or set log4j.logger.com.garmin.dsii.datahub=DEBUG to check the log
      // todo: add assert to check if lineage is really created
    }
  }

  Feature("RecoverSoftDelete") {
    Scenario("RecoverSoftDelete") {
      val emitter = RestEmitterUtil.init("http://linxta-pesmack02.garmin.com:8080")

      RestEmitterUtil.emit(new Urn("urn:li:dataFlow:(spark,DatahubSparkListener-Test!,local[1000])"), new Status().setRemoved(false), emitter)
    }
  }
}