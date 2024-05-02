package com.garmin.dsii.datahub.utils

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

object OpenApiUtil {

  private val LOGGER = LoggerFactory.getLogger(getClass.getName)
  lazy val httpClient: CloseableHttpClient = HttpClients.createDefault()

  def getRelationships(url: String, urn: String): String = {
    val httpGet = new HttpGet(s"$url/openapi/relationships/v1/?urn=$urn&relationshipTypes=IsPartOf&direction=INCOMING&start=0&count=200")
    val response = httpClient.execute(httpGet)
    val responseBody = EntityUtils.toString(response.getEntity)

    responseBody
  }

  def deleteEntityByUrn(url: String, urn: String) = {

    val body = "{\"urn\":" + urn +"}"
    val httpPost = new HttpPost(s"$url/entities?action=delete")

    LOGGER.info(s"delete: $body")
    httpPost.setEntity(new StringEntity(body))
    val response = httpClient.execute(httpPost)
    LOGGER.info(EntityUtils.toString(response.getEntity))
  }

}
