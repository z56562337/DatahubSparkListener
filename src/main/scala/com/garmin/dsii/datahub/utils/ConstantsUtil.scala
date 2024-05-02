package com.garmin.dsii.datahub.utils

import com.google.common.collect.ImmutableSet
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}

import java.util

object ConstantsUtil {

  val HDFS_INPUT_IS_FILE_FLAG = "spark.hdfs.ingestion.input.path"
  val HDFS_OUTPUT_IS_FILE_FLAG = "spark.hdfs.ingestion.output.path"
  val EVENT_CMD: util.Set[Class[_ <: LogicalPlan]] =
    ImmutableSet.of(
      classOf[SaveIntoDataSourceCommand], // insert jdbc
      classOf[CreateDataSourceTableAsSelectCommand], // not found this situation
      classOf[CreateHiveTableAsSelectCommand], // create hive table
      classOf[InsertIntoHiveTable], // insert hive
      classOf[HiveTableRelation], // hive text format query
      classOf[LogicalRelation], // hive orc,parquet format, hdfs, jdbc query
      classOf[InsertIntoHadoopFsRelationCommand], // insert hdfs
      classOf[InMemoryRelation] // cache datasource
    )
}
