package com.garmin.dsii.datahub.utils

import com.linkedin.common.FabricType
import com.linkedin.common.urn.DataPlatformUrn
import com.linkedin.dataset.DatasetProperties
import com.linkedin.schema.SchemaFieldDataType.Type
import com.linkedin.schema.{BooleanType, DateType, NullType, StringType, _}
import datahub.spark.model.LineageUtils
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ArrayBuffer

object HDFSUtil {

  private val LOGGER = LoggerFactory.getLogger(getClass.getName)

  def emitHdfsFiles(hdfsMap: collection.mutable.Map[String, List[String]], schemaMap: collection.mutable.Map[String, DataType], emitterHost: String, env: FabricType): Unit = {
    hdfsMap.foreach(x => LOGGER.info(s"hdfsMap item: ${x._1}, ${x._2}"))
    schemaMap.foreach(x => LOGGER.info(s"schemaMap item: ${x._1}, ${x._2}"))

    val emitter = RestEmitterUtil.init(emitterHost)

    try {
      hdfsMap.foreach { case (name: String, cols: List[String]) =>
        if (emitter.testConnection()) {
          val dataset = LineageUtils.createDatasetUrn("hdfs", null, name, env)
          val schemaMetadata = new SchemaMetadata().setSchemaName(s"${name}_schema").setVersion(1).setHash("").setPlatformSchema(SchemaMetadata.PlatformSchema.create(new KafkaSchema().setDocumentSchema(""))).setPlatform(new DataPlatformUrn("hdfs"))

          val arrayBuffer = ArrayBuffer.empty[SchemaField].asJava
          cols.foreach { col =>
            val schemaField = new SchemaField()
            val dataType = schemaMap.getOrElse(col, ObjectType)
            var fieldDataType: Type = null
            var nativeDataType: String = null

            dataType match {
              case _: NullType =>
                fieldDataType = SchemaFieldDataType.Type.create(new com.linkedin.schema.NullType)
                nativeDataType = "Null"
              case _: DateType =>
                fieldDataType = SchemaFieldDataType.Type.create(new com.linkedin.schema.DateType)
                nativeDataType = "Date"
              case _: CalendarIntervalType =>
                fieldDataType = SchemaFieldDataType.Type.create(new com.linkedin.schema.DateType)
                nativeDataType = "Calendar"
              case _: TimestampType =>
                fieldDataType = SchemaFieldDataType.Type.create(new TimeType)
                nativeDataType = "Timestamp"
              case _: ByteType =>
                fieldDataType = SchemaFieldDataType.Type.create(new BytesType)
                nativeDataType = "Byte"
              case _: BinaryType =>
                fieldDataType = SchemaFieldDataType.Type.create(new BytesType)
                nativeDataType = "Binary"
              case _: BooleanType =>
                fieldDataType = SchemaFieldDataType.Type.create(new com.linkedin.schema.BooleanType)
                nativeDataType = "Boolean"
              case _: IntegerType =>
                fieldDataType = SchemaFieldDataType.Type.create(new NumberType)
                nativeDataType = "Integer"
              case _: LongType =>
                fieldDataType = SchemaFieldDataType.Type.create(new NumberType)
                nativeDataType = "Long"
              case _: DoubleType =>
                fieldDataType = SchemaFieldDataType.Type.create(new NumberType)
                nativeDataType = "Double"
              case _: FloatType =>
                fieldDataType = SchemaFieldDataType.Type.create(new NumberType)
                nativeDataType = "Float"
              case _: ShortType =>
                fieldDataType = SchemaFieldDataType.Type.create(new NumberType)
                nativeDataType = "Short"
              case _: StringType =>
                fieldDataType = SchemaFieldDataType.Type.create(new com.linkedin.schema.StringType)
                nativeDataType = "String"
              case _ =>
                fieldDataType = SchemaFieldDataType.Type.create(new com.linkedin.schema.NullType)
                nativeDataType = "New Column"
            }

            schemaField.setFieldPath(col).setType(new SchemaFieldDataType().setType(fieldDataType)).setNativeDataType(nativeDataType)
            arrayBuffer.add(schemaField)
          }

          schemaMetadata.setFields(new SchemaFieldArray(arrayBuffer))
          LOGGER.info(s"schemaMetadata: ${schemaMetadata.getFields}")
          RestEmitterUtil.emit(dataset, new DatasetProperties().setName(name), emitter)
          RestEmitterUtil.emit(dataset, schemaMetadata, emitter)
        }
      }
    } catch {
      case e: Exception =>
        LOGGER.info(s"Message: ${e.getMessage}")
        LOGGER.info(s"StackTrace : ${e.printStackTrace()}")
    } finally {
      emitter.close()
    }
  }
}
