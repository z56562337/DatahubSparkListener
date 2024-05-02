package com.garmin.dsii.datahub.listener

import com.fasterxml.jackson.databind.node.ArrayNode
import com.garmin.dsii.datahub.dto.DataLineage
import com.garmin.dsii.datahub.utils.{ConstantsUtil, OpenApiUtil, RestEmitterUtil}
import com.linkedin.data.template.StringMap
import com.linkedin.datajob.{DataFlowInfo, DataJobInfo, DataJobInputOutput}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEvent}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.slf4j.LoggerFactory

import java.util.Date

class DatahubSparkListener extends SparkListener {


  private val LOGGER = LoggerFactory.getLogger(getClass.getName)
  var isFirstEvent = true


  private class SqlStartTask(val sqlStart: SparkListenerSQLExecutionStart, val plan: LogicalPlan, val ctx: SparkContext) {

    def run(): Unit = {
      if (ctx == null) {
        LOGGER.error("Context is null skipping run")
        return
      }
      if (ctx.getConf == null) {
        LOGGER.error("Context does not have config. Skipping run")
        return
      }
      if (sqlStart == null) {
        LOGGER.error("sqlStart is null skipping run")
        return
      }

      if (ConstantsUtil.EVENT_CMD.contains(plan.getClass)) {
        try {
          val dataLineage = DatasetExtractor.extractLineage(sqlStart, plan, ctx)
          createDataLineage(sqlStart, ctx, dataLineage)
        } catch {
          case e: Exception =>
            LOGGER.info(e.getMessage)
        }

        LOGGER.info("PLAN for execution id: " + ctx.appName + ":" + sqlStart.executionId + "\n")
      }
    }
  }

  private def createDataLineage(sqlStart: SparkListenerSQLExecutionStart, ctx: SparkContext, dataLineage: DataLineage): Unit = {

    val emitter = RestEmitterUtil.init(ctx.getConf.get("spark.garmin.datahub.rest.server"))

    try {
      if (emitter.testConnection()) {

        val jobIO: DataJobInputOutput = new DataJobInputOutput().setInputDatasets(dataLineage.inputDatasets)
          .setOutputDatasets(dataLineage.outputDatasets)

        val customProps = new StringMap
        customProps.put("startedAt", new Date(sqlStart.time).toString)
        customProps.put("appId", sqlStart.executionId.toString)
        customProps.put("appName", ctx.appName)

        val jobInfo: DataJobInfo = new DataJobInfo()
          .setName(sqlStart.description)
          .setType(DataJobInfo.Type.create("sparkJob"))
          .setCustomProperties(customProps)

        if (isFirstEvent) {
          val url = ctx.getConf.get("spark.garmin.datahub.rest.server")
          val urn = s"urn:li:dataFlow:(spark,${ctx.appName},${ctx.master})"
          LOGGER.info(s"dataFlow urn: $urn")

          val relationships = OpenApiUtil.getRelationships(url, urn)
          val json = com.garmin.dsii.Json.JsonUtils.toJsonNode(relationships)
          val jsonArray = json.get("entities").asInstanceOf[ArrayNode]
          val iterator = jsonArray.elements()
          while (iterator.hasNext) {
            OpenApiUtil.deleteEntityByUrn(url, iterator.next().get("urn").toString)
          }

          RestEmitterUtil.emit(dataLineage.dataFlow, new DataFlowInfo().setName(ctx.appName), emitter)
          isFirstEvent = false
        }
        RestEmitterUtil.emit(dataLineage.dataJobUrn, jobInfo, emitter)
        RestEmitterUtil.emit(dataLineage.dataJobUrn, jobIO, emitter)

      }
    } catch {
      case e: Exception =>
        LOGGER.info(s"Message: ${e.getMessage}")
        LOGGER.info(s"StackTrace : ${e.printStackTrace()}")
    } finally {
      emitter.close()
    }
  }


  private def processExecution(sqlStart: SparkListenerSQLExecutionStart): Unit = {
    val queryExec = SQLExecution.getQueryExecution(sqlStart.executionId)
    if (queryExec == null) {
      LOGGER.info("Skipping  for sql exec Id" + sqlStart.executionId + " as Query execution context could not be read from current spark state")
      return
    }
    val plan = queryExec.optimizedPlan
    val sess = queryExec.sparkSession
    val ctx = sess.sparkContext
    new SqlStartTask(sqlStart, plan, ctx).run()
  }


  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case sqlEvt: SparkListenerSQLExecutionStart =>
        LOGGER.info(s"SQL Exec start event with id: ${sqlEvt.executionId}")
        LOGGER.info("=======================================================================================")
        processExecution(sqlEvt)
      case _ =>
    }
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    LOGGER.info(s"Application Start at ${applicationStart.time}")
    LOGGER.info(s"sparkUser: ${applicationStart.sparkUser}")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    LOGGER.info(s"Application End at ${applicationEnd.time}")
  }
}
