package com.github.mmolimar.kukulcan

import _root_.java.util.{Collections, Properties, Arrays => JArrays}

import _root_.com.github.mmolimar.kukulcan.java.{KSchemaRegistry => JKSchemaRegistry}
import _root_.com.github.mmolimar.kukulcan.{KSchemaRegistry => SKSchemaRegistry}
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig}

class KSchemaRegistrySpec extends KukulcanApiTestHarness with EmbeddedKafka {

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = KAFKA_PORT,
    zooKeeperPort = ZOOKEEPER_PORT,
    schemaRegistryPort = SCHEMA_REGISTRY_PORT,
    customSchemaRegistryProperties = Map(
      SchemaRegistryConfig.MODE_MUTABILITY -> "true"
    )
  )

  val subject: String = "test"
  val version: Int = 12345
  val id: Int = 999
  val avroSchema: String =
    """
      |{
      |  "namespace": "kukulcan",
      |  "type": "record",
      |  "name": "sample",
      |  "fields": [
      |    {
      |      "name": "field1",
      |      "type": "string",
      |      "default": null
      |    }
      |  ]
      |}
      |""".stripMargin
  val compatibleSchema: String =
    """
      |{
      |  "namespace": "kukulcan",
      |  "type": "record",
      |  "name": "compatible",
      |  "fields": [
      |    {
      |      "name": "field1",
      |      "type": ["null", "string"],
      |      "default": null
      |    },
      |    {
      |      "name": "field2",
      |      "type": ["null", "string"],
      |      "default": null
      |    }
      |  ]
      |}
      |""".stripMargin
  val incompatibleSchema: String =
    """
      |{
      |  "namespace": "kukulcan",
      |  "type": "record",
      |  "name": "incompatible",
      |  "fields": [
      |    {
      |      "name": "field2",
      |      "type": "string"
      |    }
      |  ]
      |}
      |""".stripMargin


  override def apiClass: Class[_] = classOf[SKSchemaRegistry]

  override def execScalaTests(): Unit = withRunningKafka {
    val scalaApi: SKSchemaRegistry = {
      val props = new Properties()
      props.put("schema.registry.url", s"http://localhost:$SCHEMA_REGISTRY_PORT")
      SKSchemaRegistry(props)
    }

    val parsedSchema = scalaApi.parseSchema("AVRO", avroSchema, Seq.empty).get
    val compatibleParsedSchema = scalaApi.parseSchema("AVRO", compatibleSchema, Seq.empty).get
    val incompatibleParsedSchema = scalaApi.parseSchema("AVRO", incompatibleSchema, Seq.empty).get

    scalaApi.getMode shouldBe "READWRITE"
    assertThrows[RestClientException] {
      scalaApi.register(subject, parsedSchema, version, id)
    }
    scalaApi.setMode("IMPORT") shouldBe "IMPORT"
    scalaApi.getMode shouldBe "IMPORT"
    scalaApi.register(subject, parsedSchema, version, id) shouldBe id
    scalaApi.setMode("READWRITE", subject) shouldBe "READWRITE"
    scalaApi.register(subject, compatibleParsedSchema) shouldBe (id + 1)
    scalaApi.getMode(subject) shouldBe "READWRITE"
    assertThrows[RestClientException] {
      scalaApi.setMode("IMPORT", subject)
    }
    scalaApi.setMode("IMPORT", "not_existent") shouldBe "IMPORT"

    scalaApi.getSchema(id) shouldBe parsedSchema
    scalaApi.getSchema(id, subject) shouldBe parsedSchema

    scalaApi.getAllSubjects.size shouldBe 1
    scalaApi.getAllSubjects.head shouldBe subject
    scalaApi.getAllSubjectsById(id).size shouldBe 1
    scalaApi.getAllSubjectsById(id).head shouldBe subject

    scalaApi.getAllVersions(subject).size shouldBe 2
    scalaApi.getAllVersions(subject) shouldBe Seq(version, version + 1)
    scalaApi.getAllVersionsById(id).size shouldBe 1
    scalaApi.getAllVersionsById(id).head shouldBe new SubjectVersion(subject, version)
    scalaApi.getByVersion(subject, version, lookupDeletedSchema = false)

    val latestSchemaMetadata = scalaApi.getLatestSchemaMetadata(subject)
    val schemaMetadata = scalaApi.getSchemaMetadata(subject, version)
    latestSchemaMetadata.getId shouldBe (schemaMetadata.getId + 1)
    latestSchemaMetadata.getSchema shouldNot be(schemaMetadata.getSchema)
    latestSchemaMetadata.getSchemaType shouldBe schemaMetadata.getSchemaType
    latestSchemaMetadata.getVersion shouldBe (schemaMetadata.getVersion + 1)
    latestSchemaMetadata.getReferences shouldBe schemaMetadata.getReferences

    scalaApi.testCompatibility(subject, incompatibleParsedSchema) shouldBe false
    scalaApi.testCompatibility(subject, compatibleParsedSchema) shouldBe true
    scalaApi.testCompatibility(subject, parsedSchema) shouldBe false
    scalaApi.updateCompatibility(subject, "FULL") shouldBe "FULL"
    scalaApi.getCompatibility(subject) shouldBe "FULL"

    scalaApi.getId(subject, parsedSchema) shouldBe id
    scalaApi.getVersion(subject, parsedSchema) shouldBe version

    scalaApi.deleteSchemaVersion(subject, version.toString) shouldBe version
    assertThrows[RestClientException] {
      scalaApi.deleteSchemaVersion(subject, version.toString, Map.empty)
    }
    scalaApi.deleteSubject(subject) shouldBe Seq(version + 1)
    assertThrows[RestClientException] {
      scalaApi.deleteSubject(subject, Map.empty)
    }

    scalaApi.reset()
  }

  override def execJavaTests(): Unit = withRunningKafka {
    val javaApi: JKSchemaRegistry = {
      val props = new Properties()
      props.put("schema.registry.url", s"http://localhost:$SCHEMA_REGISTRY_PORT")
      new JKSchemaRegistry(props)
    }

    val parsedSchema = javaApi.parseSchema("AVRO", avroSchema, Collections.emptyList()).get
    val compatibleParsedSchema = javaApi.parseSchema("AVRO", compatibleSchema, Collections.emptyList()).get
    val incompatibleParsedSchema = javaApi.parseSchema("AVRO", incompatibleSchema, Collections.emptyList()).get

    javaApi.getMode shouldBe "READWRITE"
    assertThrows[RestClientException] {
      javaApi.register(subject, parsedSchema, version, id)
    }
    javaApi.setMode("IMPORT") shouldBe "IMPORT"
    javaApi.getMode shouldBe "IMPORT"
    javaApi.register(subject, parsedSchema, version, id) shouldBe id
    javaApi.setMode("READWRITE", subject) shouldBe "READWRITE"
    javaApi.register(subject, compatibleParsedSchema) shouldBe (id + 1)
    javaApi.getMode(subject) shouldBe "READWRITE"
    assertThrows[RestClientException] {
      javaApi.setMode("IMPORT", subject)
    }
    javaApi.setMode("IMPORT", "not_existent") shouldBe "IMPORT"

    javaApi.getSchema(id) shouldBe parsedSchema
    javaApi.getSchema(id, subject) shouldBe parsedSchema

    javaApi.getAllSubjects.size shouldBe 1
    javaApi.getAllSubjects.get(0) shouldBe subject
    javaApi.getAllSubjectsById(id).size shouldBe 1
    javaApi.getAllSubjectsById(id).get(0) shouldBe subject

    javaApi.getAllVersions(subject).size shouldBe 2
    javaApi.getAllVersions(subject) shouldBe JArrays.asList(version, version + 1)
    javaApi.getAllVersionsById(id).size shouldBe 1
    javaApi.getAllVersionsById(id).get(0) shouldBe new SubjectVersion(subject, version)
    javaApi.getByVersion(subject, version, false)

    val latestSchemaMetadata = javaApi.getLatestSchemaMetadata(subject)
    val schemaMetadata = javaApi.getSchemaMetadata(subject, version)
    latestSchemaMetadata.getId shouldBe (schemaMetadata.getId + 1)
    latestSchemaMetadata.getSchema shouldNot be(schemaMetadata.getSchema)
    latestSchemaMetadata.getSchemaType shouldBe schemaMetadata.getSchemaType
    latestSchemaMetadata.getVersion shouldBe (schemaMetadata.getVersion + 1)
    latestSchemaMetadata.getReferences shouldBe schemaMetadata.getReferences

    javaApi.testCompatibility(subject, incompatibleParsedSchema) shouldBe false
    javaApi.testCompatibility(subject, compatibleParsedSchema) shouldBe true
    javaApi.testCompatibility(subject, parsedSchema) shouldBe false
    javaApi.updateCompatibility(subject, "FULL") shouldBe "FULL"
    javaApi.getCompatibility(subject) shouldBe "FULL"

    javaApi.getId(subject, parsedSchema) shouldBe id
    javaApi.getVersion(subject, parsedSchema) shouldBe version

    javaApi.deleteSchemaVersion(subject, version.toString) shouldBe version
    assertThrows[RestClientException] {
      javaApi.deleteSchemaVersion(subject, version.toString, Collections.emptyMap())
    }
    javaApi.deleteSubject(subject) shouldBe JArrays.asList(version + 1)
    assertThrows[RestClientException] {
      javaApi.deleteSubject(subject, Collections.emptyMap())
    }

    javaApi.reset()
  }

}
