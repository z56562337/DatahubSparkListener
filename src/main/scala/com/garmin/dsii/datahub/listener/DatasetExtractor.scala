package com.garmin.dsii.datahub.listener

import com.garmin.dsii.datahub.dto.DataLineage
import com.garmin.dsii.datahub.utils.{ConstantsUtil, HDFSUtil}
import com.linkedin.common.urn.DataJobUrn
import com.linkedin.common.{DatasetUrnArray, FabricType}
import datahub.spark.model.LineageUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.sql.execution.{FileSourceScanExec, RowDataSourceScanExec}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.slf4j.LoggerFactory

object DatasetExtractor {

  private val LOGGER = LoggerFactory.getLogger(getClass.getName)
  private val schemaMap = collection.mutable.Map.empty[String, DataType]
  private val hdfsMap = collection.mutable.Map.empty[String, List[String]]
  var hdfsInputPath = ""
  var hdfsOutputPath = ""


  def extractLineage(sqlStart: SparkListenerSQLExecutionStart, plan: LogicalPlan, ctx: SparkContext): DataLineage = {
    val inputDatasets = new DatasetUrnArray()
    val outputDatasets = new DatasetUrnArray()
    val dataFlow = LineageUtils.flowUrn(ctx.master, ctx.appName)
    val dataJobUrn = new DataJobUrn(dataFlow, s"ExecutionId_${sqlStart.executionId}")

    val host = ctx.getConf.get("fs.defaultFS")
    val env: FabricType = if (host.contains("stag") || host.contains("linxta")) FabricType.DEV else FabricType.PROD
    hdfsInputPath = if (ctx.getConf.contains(ConstantsUtil.HDFS_INPUT_IS_FILE_FLAG)) ctx.getConf.get(ConstantsUtil.HDFS_INPUT_IS_FILE_FLAG) else ""
    hdfsOutputPath = if (ctx.getConf.contains(ConstantsUtil.HDFS_OUTPUT_IS_FILE_FLAG)) ctx.getConf.get(ConstantsUtil.HDFS_OUTPUT_IS_FILE_FLAG) else ""
    val description = sqlStart.description
    schemaMap.clear()
    hdfsMap.clear()

    LOGGER.info("env: " + env)
    LOGGER.info("hdfsInputPath: " + hdfsInputPath)
    LOGGER.info("hdfsOutputPath: " + hdfsOutputPath)


    plan.foreach { p: LogicalPlan =>
      if (ConstantsUtil.EVENT_CMD.contains(p.getClass)) {
        LOGGER.info("***")
        LOGGER.info(s"Class Type: ${p.getClass.toString}")
        LOGGER.info(s"Insert Code: $description")
        LOGGER.info("==========================================================================================")

        p match {
          case logicalPlan: InsertIntoHadoopFsRelationCommand =>
            val catalogTable = logicalPlan.catalogTable

            if (catalogTable.isDefined) {
              val database = catalogTable.get.identifier.database.getOrElse("")
              val table = catalogTable.get.identifier.table
              val outputTable = if (database != "") s"$database.$table" else table

              LOGGER.info(s"database: $database")
              LOGGER.info(s"table: $table")

              addToDatasetUrnArray("output", "hive", outputTable, outputDatasets, env)
            } else {
              val path = logicalPlan.outputPath.toUri.toString

              LOGGER.info(s"path: $path")
              logicalPlan.outputColumns.foreach(x => LOGGER.info(s"outputColumns: ${x.toString().substring(0, x.toString().indexOf("#"))}"))

              val outputPath = if (hdfsOutputPath.equals("")) path else hdfsOutputPath

              addToDatasetUrnArray("output", "hdfs", outputPath, outputDatasets, env)
              hdfsMap.update(outputPath, logicalPlan.outputColumns.toList.map(x => x.toString().substring(0, x.toString().indexOf("#"))))
            }

            extractRelationInInsertCommand(logicalPlan.query, env, inputDatasets)

          case logicalPlan: SaveIntoDataSourceCommand =>
            val options = logicalPlan.options
            val url = options("url")
            val dbtable = options("dbtable")
            val platform = extractPlatformFromJDBCUrl(url)

            LOGGER.info(s"url: $url")
            LOGGER.info(s"dbtable: $dbtable")
            LOGGER.info(s"platform: $platform")

            addToDatasetUrnArray("output", platform, dbtable, outputDatasets, env)
            extractRelationInInsertCommand(logicalPlan.query, env, inputDatasets)

          case logicalPlan: InsertIntoHiveTable =>
            val owner = logicalPlan.table.owner
            val database = logicalPlan.table.identifier.database.getOrElse("")
            val table = logicalPlan.table.identifier.table
            val outputTable = if (database != "") s"$database.$table" else table

            LOGGER.info(s"owner: $owner")
            LOGGER.info(s"database: $database")
            LOGGER.info(s"table: $table")

            addToDatasetUrnArray("output", "hive", outputTable, outputDatasets, env)
            extractRelationInInsertCommand(logicalPlan.query, env, inputDatasets)

          case logicalPlan: CreateHiveTableAsSelectCommand =>
            val owner = logicalPlan.tableDesc.owner
            val database = logicalPlan.tableDesc.identifier.database.getOrElse("")
            val table = logicalPlan.tableDesc.identifier.table
            val outputTable = if (database != "") s"$database.$table" else table

            LOGGER.info(s"owner: $owner")
            LOGGER.info(s"database: $database")
            LOGGER.info(s"table: $table")

            addToDatasetUrnArray("output", "hive", outputTable, outputDatasets, env)
            extractRelationInInsertCommand(logicalPlan.query, env, inputDatasets)
          case logicalPlan: MultiInstanceRelation =>
            extractMultiInstanceRelation(logicalPlan, env, inputDatasets)
          case _ =>
        }
      }
    }

    if (hdfsMap.nonEmpty)
      HDFSUtil.emitHdfsFiles(hdfsMap, schemaMap, ctx.getConf.get("spark.garmin.datahub.rest.server"), env)

    DataLineage(inputDatasets, outputDatasets, dataFlow, dataJobUrn)
  }

  def extractBaseRelation(relation: BaseRelation, env: FabricType, inputDatasets: DatasetUrnArray) = {
    relation match {
      case r: HadoopFsRelation =>
        val rootPaths = r.location.rootPaths

        LOGGER.info(s"HadoopFsRelation rootPaths: $rootPaths")

        rootPaths.foreach { p =>
          val path = p.toString
          LOGGER.info(s"path: $path")

          if (!path.contains("hive")) {
            val inputPath = if (hdfsInputPath.equals("")) path else hdfsInputPath

            addToDatasetUrnArray("input", "hdfs", inputPath, inputDatasets, env)
            hdfsMap.update(inputPath, r.schema.toList.map(x => x.name))
          } else {
            val pathElements = path.split("/")
            val table = s"${pathElements(pathElements.length - 2).replace("db", "")}${pathElements(pathElements.length - 1)}"
            LOGGER.info(s"table: $table")

            addToDatasetUrnArray("input", "hive", table, inputDatasets, env)
          }
        }

        extractInputSchema(r.schema)
      case _ =>
        if (relation.getClass.toString.contains("JDBCRelation")) {
          // get JDBCRelation class
          val jdbcRelationClass = Class.forName("org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation")

          // get JDBCRelation -> JDBCOptions reflection
          val field = jdbcRelationClass.getDeclaredField("jdbcOptions")
          field.setAccessible(true)
          val value = field.get(relation).asInstanceOf[JDBCOptions]
          val url = value.url
          val table = value.parameters.get(JDBCOptions.JDBC_TABLE_NAME).get
          val platform = extractPlatformFromJDBCUrl(url)

          LOGGER.info(s"url: $url")
          LOGGER.info(s"JDBC_TABLE_NAME: $table")
          LOGGER.info(s"platform: $platform")

          addToDatasetUrnArray("input", platform, table, inputDatasets, env)
          extractInputSchema(relation.schema)
        }
    }
  }

  def extractMultiInstanceRelation(relation: MultiInstanceRelation, env: FabricType, inputDatasets: DatasetUrnArray): Unit = {
    relation match {
      case logicalPlan: HiveTableRelation =>
        val database = logicalPlan.tableMeta.identifier.database.getOrElse("")
        val table = logicalPlan.tableMeta.identifier.table
        val inputTable = if (database != "") s"$database.$table" else table

        LOGGER.info(s"database: $database")
        LOGGER.info(s"table: $table")
        LOGGER.info(s"schema: ${logicalPlan.schema}")

        addToDatasetUrnArray("input", "hive", inputTable, inputDatasets, env)
        extractInputSchema(logicalPlan.schema)

      case logicalPlan: InMemoryRelation =>
        logicalPlan.cachedPlan.collectLeaves().foreach(x => LOGGER.info(s"InMemoryRelation: ${x.getClass}"))
        logicalPlan.cachedPlan.collectLeaves().foreach {
          // for hdfs
          case exec: FileSourceScanExec =>
            extractBaseRelation(exec.relation, env, inputDatasets)
          // for JDBC
          case exec: RowDataSourceScanExec =>
            extractBaseRelation(exec.relation, env, inputDatasets)
          // for cache
          case exec: InMemoryTableScanExec =>
            extractMultiInstanceRelation(exec.relation, env, inputDatasets)
          case exec =>
            if (exec.getClass.toString.contains("HiveTableScanExec")) {
              // get HiveTableScanExec class
              val hiveTableScanExec = Class.forName("org.apache.spark.sql.hive.execution.HiveTableScanExec")

              // get HiveTableScanExec -> relation: HiveTableRelation reflection
              val field = hiveTableScanExec.getDeclaredField("relation")
              field.setAccessible(true)
              // field.get(exec) == exec.relation
              val relation = field.get(exec).asInstanceOf[HiveTableRelation]


              extractMultiInstanceRelation(relation, env, inputDatasets)
            }
          case _ =>
        }

      case logicalPlan: LogicalRelation =>
        val relation = logicalPlan.relation
        val catalogTable = logicalPlan.catalogTable

        if (catalogTable.isDefined) {
          val database = catalogTable.get.identifier.database.getOrElse("")
          val table = catalogTable.get.identifier.table
          val inputTable = if (database != "") s"$database.$table" else table

          LOGGER.info(s"database: $database")
          LOGGER.info(s"table: $table")

          addToDatasetUrnArray("input", "hive", inputTable, inputDatasets, env)
          extractInputSchema(logicalPlan.schema)
        }

        extractBaseRelation(relation, env, inputDatasets)
      case _ =>
    }

  }

  def extractRelationInInsertCommand(query: LogicalPlan, env: FabricType, inputDatasets: DatasetUrnArray) = {
    query.foreach {
      case q: LogicalRelation => extractMultiInstanceRelation(q, env, inputDatasets)
      case _ =>
    }
  }

  def extractPlatformFromJDBCUrl(jdbcUrl: String): String = {
    val platformTemp = jdbcUrl.split(":")(1)
    if (platformTemp == "sqlserver") "mssql" else platformTemp
  }

  def extractInputSchema(schema: StructType): Unit = {
    schema.foreach { field: StructField =>
      schemaMap.update(field.name, field.dataType)
    }
  }

  def addToDatasetUrnArray(action: String, dataType: String, sourceName: String, dataSets: DatasetUrnArray, env: FabricType): Unit = {
    LOGGER.info(s"$sourceName add to $action datasets")
    dataSets.add(LineageUtils.createDatasetUrn(dataType, null, sourceName, env))
  }
}
