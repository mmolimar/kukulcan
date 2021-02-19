package com.github.mmolimar.kukulcan

import _root_.java.util.{Properties, Arrays => JArrays, List => JList, Map => JMap}

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.rest.entities.{Schema, SchemaReference, SubjectVersion}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider
import io.confluent.kafka.schemaregistry.{ParsedSchema, SchemaProvider}

import scala.collection.JavaConverters._

/**
 * Factory for [[com.github.mmolimar.kukulcan.KSchemaRegistry]] instances.
 *
 */
object KSchemaRegistry {

  def apply[K, V](props: Properties): KSchemaRegistry = new KSchemaRegistry(props)

}

/**
 * An enriched implementation of the {@code io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient} class
 * to manage schemas in the SchemaRegistry.
 *
 * @param props Properties with the configuration.
 */
class KSchemaRegistry(val props: Properties) {

  private val client: KCachedSchemaRegistryClient = fromProps(props)

  private def fromProps(props: Properties): KCachedSchemaRegistryClient = {
    val baseUrls: JList[String] = JArrays.asList(props.getProperty("schema.registry.url").split(",")
      .filter(_.trim.nonEmpty): _*)
    val mapCapacity = props.getOrDefault("max.schemas.per.subject", "1000").toString.toInt
    val providers: JList[SchemaProvider] = JArrays
      .asList(new AvroSchemaProvider, new JsonSchemaProvider, new ProtobufSchemaProvider)
    val httpHeaders = props.keys().asScala.filter(_.toString.startsWith("request.header."))
      .map(_.toString.substring("request.header.".length))
      .map(k => k -> props.getProperty(k)).toMap.asJava

    new KCachedSchemaRegistryClient(
      new RestService(baseUrls),
      mapCapacity,
      providers,
      props.asInstanceOf[JMap[String, String]],
      httpHeaders
    )
  }

  private class KCachedSchemaRegistryClient(
                                             restService: RestService,
                                             identityMapCapacity: Int,
                                             providers: JList[SchemaProvider],
                                             originals: JMap[String, _],
                                             httpHeaders: JMap[String, String]
                                           )
    extends CachedSchemaRegistryClient(
      restService,
      identityMapCapacity,
      providers,
      originals,
      httpHeaders
    ) {

    def testCompatibility(subject: String, schema: ParsedSchema, version: String, verbose: Boolean): Seq[String] = {
      restService.testCompatibility(
        schema.canonicalString, schema.schemaType, schema.references, subject, version, verbose
      ).asScala
    }
  }

  /**
   * Parse a string representing a schema for a specified schema type.
   *
   * @param schemaType   The schema type.
   * @param schemaString The schema content.
   * @param references   A list of schema references.
   * @return an {@code Option[ParsedSchema]} schema parsed.
   */
  def parseSchema(schemaType: String, schemaString: String, references: Seq[SchemaReference]): Option[ParsedSchema] = {
    Option(client.parseSchema(schemaType, schemaString, references.asJava).orElse(None.orNull))
  }

  /**
   * Register a schema in SchemaRegistry if not cached under the specified subject.
   *
   * @param subject The subject in which to register the schema.
   * @param schema  The schema already parsed.
   * @param version Schema version to set. Default 0.
   * @param id      Schema ID to set. Default -1.
   * @return the schema ID.
   */
  def register(subject: String, schema: ParsedSchema, version: Int = 0, id: Int = -1): Int = {
    client.register(subject, schema, version, id)
  }

  /**
   * Get the schema registered by the specified ID.
   *
   * @param id The schema ID to get.
   * @return the parsed schema.
   */
  def getSchema(id: Int): ParsedSchema = getSchema(id, None.orNull)

  /**
   * Get the schema registered by the specified ID and subject.
   *
   * @param id      The schema ID to get.
   * @param subject The subject to get the schema if needed.
   * @return the parsed schema.
   */
  def getSchema(id: Int, subject: String): ParsedSchema = client.getSchemaBySubjectAndId(subject, id)

  /**
   * Get all subjects for a specified schema ID.
   *
   * @param id The schema ID.
   * @return a list with all the subjects.
   */
  def getAllSubjectsById(id: Int): Seq[String] = client.getAllSubjectsById(id).asScala.toSeq

  /**
   * Get all subject versions for a specified schema ID.
   *
   * @param id The schema ID.
   * @return a list with all subjects versions.
   */
  def getAllVersionsById(id: Int): Seq[SubjectVersion] = client.getAllVersionsById(id).asScala.toSeq

  /**
   * Get a schema by version in SchemaRegistry.
   *
   * @param subject             The schema subject.
   * @param version             The schema version.
   * @param lookupDeletedSchema If look for deleted schemas.
   * @return a {@code Schema} with its related info.
   */
  def getByVersion(subject: String, version: Int, lookupDeletedSchema: Boolean): Schema = {
    client.getByVersion(subject, version, lookupDeletedSchema)
  }

  /**
   * Get the schema metadata for a specified subject in its latest version.
   *
   * @param subject The schema subject.
   * @return a {@code SchemaMetadata}.
   */
  def getLatestSchemaMetadata(subject: String): SchemaMetadata = client.getLatestSchemaMetadata(subject)

  /**
   * Get the schema metadata for a specified subject and version.
   *
   * @param subject The schema subject.
   * @return a {@code SchemaMetadata}.
   */
  def getSchemaMetadata(subject: String, version: Int): SchemaMetadata = client.getSchemaMetadata(subject, version)

  /**
   * Get the schema version for a parsed schema.
   *
   * @param subject The schema subject.
   * @param schema  The parsed schema.
   * @return the version of the schema.
   */
  def getVersion(subject: String, schema: ParsedSchema): Int = client.getVersion(subject, schema)

  /**
   * Get the all versions for a subject.
   *
   * @param subject The schema subject.
   * @return a list with the schema versions.
   */
  def getAllVersions(subject: String): Seq[Integer] = client.getAllVersions(subject).asScala

  /**
   * Check the compatibility for a schema in a subject for the specified version (latest by default).
   *
   * @param subject The schema subject.
   * @param schema  The parsed schema to validate.
   * @param version The schema version to validate. Latest version by default.
   * @param verbose Enable verbose messages from Schema Registry.
   * @return list with the incompatibilities, if any.
   */
  def testCompatibility(
                         subject: String,
                         schema: ParsedSchema,
                         version: String = "latest",
                         verbose: Boolean = false
                       ): Seq[String] = {
    client.testCompatibility(subject, schema, version, verbose)
  }

  /**
   * Update the compatibility for a schema subject.
   *
   * @param subject       The schema subject.
   * @param compatibility The compatibility level.
   * @return the compatibility level.
   */
  def updateCompatibility(subject: String, compatibility: String): String = {
    client.updateCompatibility(subject, compatibility)
  }

  /**
   * Get the compatibility level for a schema subject.
   *
   * @param subject The schema subject.
   * @return the compatibility level.
   */
  def getCompatibility(subject: String): String = client.getCompatibility(subject)

  /**
   * Set the mode {@code READWRITE}, {@code READONLY} or {@code IMPORT}.
   *
   * @return the current mode.
   */
  def setMode(mode: String): String = setMode(mode, None.orNull)

  /**
   * Set the mode {@code READWRITE}, {@code READONLY} or {@code IMPORT} for a subject.
   *
   * @param subject The schema subject.
   * @return the current mode.
   */
  def setMode(mode: String, subject: String): String = client.setMode(mode, subject)

  /**
   * Get the mode {@code READWRITE}, {@code READONLY} or {@code IMPORT}.
   *
   * @return the current mode.
   */
  def getMode: String = getMode(None.orNull)

  /**
   * Get the mode {@code READWRITE}, {@code READONLY} or {@code IMPORT} for a subject.
   *
   * @param subject The schema subject.
   * @return the current mode.
   */
  def getMode(subject: String): String = client.getMode(subject)

  /**
   * Get the list for all subjects registered.
   *
   * @return a list with all subjects.
   */
  def getAllSubjects: Seq[String] = client.getAllSubjects.asScala.toSeq

  /**
   * Get the ID for a specified subject and schema.
   *
   * @param subject The schema subject.
   * @param schema  The parsed schema.
   * @return a list with all subjects.
   */
  def getId(subject: String, schema: ParsedSchema): Int = client.getId(subject, schema)

  /**
   * Delete all versions of the schema registered under a subject.
   *
   * @param subject The schema subject to delete.
   * @return a list with all versions deleted.
   */
  def deleteSubject(subject: String): Seq[Integer] = client.deleteSubject(subject).asScala

  /**
   * Delete all versions of the schema registered under a subject.
   *
   * @param subject           The schema subject to delete.
   * @param requestProperties Properties to set for the request.
   * @return a list with all versions deleted.
   */
  def deleteSubject(subject: String, requestProperties: Map[String, String]): Seq[Integer] = {
    client.deleteSubject(requestProperties.asJava, subject).asScala
  }

  /**
   * Delete a version of the schema registered under a subject.
   *
   * @param subject The schema subject to delete.
   * @param version The version to delete.
   * @return the ID of the deleted version.
   */
  def deleteSchemaVersion(subject: String, version: String): Integer = client.deleteSchemaVersion(subject, version)

  /**
   * Delete a version of the schema registered under a subject.
   *
   * @param subject           The schema subject to delete.
   * @param version           The version to delete.
   * @param requestProperties Properties to set for the request.
   * @return the ID of the deleted version.
   */
  def deleteSchemaVersion(subject: String, version: String, requestProperties: Map[String, String]): Integer = {
    client.deleteSchemaVersion(requestProperties.asJava, subject, version)
  }

  /**
   * Reset all content in the cache.
   *
   */
  def reset(): Unit = client.reset()

}
