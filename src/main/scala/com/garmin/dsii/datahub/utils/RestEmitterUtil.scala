package com.garmin.dsii.datahub.utils

import com.linkedin.common.urn.Urn
import com.linkedin.data.template.DataTemplate
import datahub.client.rest.RestEmitter
import datahub.client.rest.RestEmitterConfig.RestEmitterConfigBuilder
import datahub.event.MetadataChangeProposalWrapper
import org.slf4j.LoggerFactory

import java.util.function.Consumer

object RestEmitterUtil {

  private val LOGGER = LoggerFactory.getLogger(getClass.getName)

  def init(server: String): RestEmitter = {
    RestEmitter.create(new Consumer[RestEmitterConfigBuilder] {
      override def accept(t: RestEmitterConfigBuilder): Unit = {
        t.server(server)
      }
    })
  }

  def createMCPW(entityType: String, datasetUrn: Urn, dataTemplate: DataTemplate[_ <: AnyRef]): MetadataChangeProposalWrapper.Build = {
    MetadataChangeProposalWrapper.builder().entityType(entityType).entityUrn(datasetUrn).upsert().aspect(dataTemplate)
  }

  def emit(datasetUrn: Urn, dataTemplate: DataTemplate[_ <: AnyRef], emitter: RestEmitter): Unit = {
    val future = emitter.emit(createMCPW(datasetUrn.getEntityType, datasetUrn, dataTemplate).build()).get()
    if (future.isSuccess)
      LOGGER.info(s"emit success: ${datasetUrn.getNSS}")
    else
      LOGGER.info(s"emit error: ${future.getResponseContent}")
  }
}
