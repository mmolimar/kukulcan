package com.github.mmolimar.kukulcan.java;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.github.mmolimar.kukulcan.java.KUtils.*;

/**
 * An enriched implementation of the {@code io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient} class
 * to manage schemas in the SchemaRegistry.
 */
public class KSchemaRegistry {

    private final com.github.mmolimar.kukulcan.KSchemaRegistry kschemaRegistry;

    /**
     * @param props Properties with the configuration.
     */
    public KSchemaRegistry(Properties props) {
        this.kschemaRegistry = new com.github.mmolimar.kukulcan.KSchemaRegistry(props);
    }

    /**
     * Parse a string representing a schema for a specified schema type.
     *
     * @param schemaType   The schema type.
     * @param schemaString The schema content.
     * @param references   A list of schema references.
     * @return an {@code Optional[ParsedSchema]} schema parsed.
     */
    public Optional<ParsedSchema> parseSchema(String schemaType, String schemaString, List<SchemaReference> references) {
        return toJavaOption(kschemaRegistry.parseSchema(schemaType, schemaString, toScalaSeq(references)));
    }

    /**
     * Register a schema in SchemaRegistry if not cached under the specified subject.
     *
     * @param subject The subject in which to register the schema.
     * @param schema  The schema already parsed.
     * @return the schema ID.
     */
    public int register(String subject, ParsedSchema schema) {
        return register(subject, schema, 0, -1);
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
    public int register(String subject, ParsedSchema schema, int version, int id) {
        return kschemaRegistry.register(subject, schema, version, id);
    }

    /**
     * Get the schema registered by the specified ID.
     *
     * @param id The schema ID to get.
     * @return the parsed schema.
     */
    public ParsedSchema getSchema(int id) {
        return kschemaRegistry.getSchema(id);
    }

    /**
     * Get the schema registered by the specified ID and subject.
     *
     * @param id      The schema ID to get.
     * @param subject The subject to get the schema if needed.
     * @return the parsed schema.
     */
    public ParsedSchema getSchema(int id, String subject) {
        return kschemaRegistry.getSchema(id, subject);
    }

    /**
     * Get all subjects for a specified schema ID.
     *
     * @param id The schema ID.
     * @return a list with all the subjects.
     */
    public List<String> getAllSubjectsById(int id) {
        return toJavaList(kschemaRegistry.getAllSubjectsById(id));
    }

    /**
     * Get all subject versions for a specified schema ID.
     *
     * @param id The schema ID.
     * @return a list with all subjects versions.
     */
    public List<SubjectVersion> getAllVersionsById(int id) {
        return toJavaList(kschemaRegistry.getAllVersionsById(id));
    }

    /**
     * Get a schema by version in SchemaRegistry.
     *
     * @param subject             The schema subject.
     * @param version             The schema version.
     * @param lookupDeletedSchema If look for deleted schemas.
     * @return a {@code Schema} with its related info.
     */
    public Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
        return kschemaRegistry.getByVersion(subject, version, lookupDeletedSchema);
    }

    /**
     * Get the schema metadata for a specified subject in its latest version.
     *
     * @param subject The schema subject.
     * @return a {@code SchemaMetadata}.
     */
    public SchemaMetadata getLatestSchemaMetadata(String subject) {
        return kschemaRegistry.getLatestSchemaMetadata(subject);
    }

    /**
     * Get the schema metadata for a specified subject and version.
     *
     * @param subject The schema subject.
     * @return a {@code SchemaMetadata}.
     */
    public SchemaMetadata getSchemaMetadata(String subject, int version) {
        return kschemaRegistry.getSchemaMetadata(subject, version);
    }

    /**
     * Get the schema version for a parsed schema.
     *
     * @param subject The schema subject.
     * @param schema  The parsed schema.
     * @return the version of the schema.
     */
    public int getVersion(String subject, ParsedSchema schema) {
        return kschemaRegistry.getVersion(subject, schema);
    }

    /**
     * Get the all versions for a subject.
     *
     * @param subject The schema subject.
     * @return a list with the schema versions.
     */
    public List<Integer> getAllVersions(String subject) {
        return toJavaList(kschemaRegistry.getAllVersions(subject));
    }

    /**
     * Check the compatibility for a schema in a subject for the latest version.
     *
     * @param subject The schema subject.
     * @param schema  The parsed schema to validate.
     * @return if the schema was validated or not.
     */
    public boolean testCompatibility(String subject, ParsedSchema schema) {
        return testCompatibility(subject, schema, "latest");
    }

    /**
     * Check the compatibility for a schema in a subject for the specified version.
     *
     * @param subject The schema subject.
     * @param schema  The parsed schema to validate.
     * @param version The schema version to validate. Latest version by default.
     * @return if the schema was validated or not.
     */
    public boolean testCompatibility(String subject, ParsedSchema schema, String version) {
        return kschemaRegistry.testCompatibility(subject, schema, version);
    }

    /**
     * Update the compatibility for a schema subject.
     *
     * @param subject       The schema subject.
     * @param compatibility The compatibility level.
     * @return the compatibility level.
     */
    public String updateCompatibility(String subject, String compatibility) {
        return kschemaRegistry.updateCompatibility(subject, compatibility);
    }

    /**
     * Get the compatibility level for a schema subject.
     *
     * @param subject The schema subject.
     * @return the compatibility level.
     */
    public String getCompatibility(String subject) {
        return kschemaRegistry.getCompatibility(subject);
    }

    /**
     * Set the mode {@code READWRITE}, {@code READONLY} or {@code IMPORT}.
     *
     * @return the current mode.
     */
    public String setMode(String mode) {
        return kschemaRegistry.setMode(mode);
    }

    /**
     * Set the mode {@code READWRITE}, {@code READONLY} or {@code IMPORT} for a subject.
     *
     * @param subject The schema subject.
     * @return the current mode.
     */
    public String setMode(String mode, String subject) {
        return kschemaRegistry.setMode(mode, subject);
    }

    /**
     * Get the mode {@code READWRITE}, {@code READONLY} or {@code IMPORT}.
     *
     * @return the current mode.
     */
    public String getMode() {
        return kschemaRegistry.getMode();
    }

    /**
     * Get the mode {@code READWRITE}, {@code READONLY} or {@code IMPORT} for a subject.
     *
     * @param subject The schema subject.
     * @return the current mode.
     */
    public String getMode(String subject) {
        return kschemaRegistry.getMode(subject);
    }

    /**
     * Get the list for all subjects registered.
     *
     * @return a list with all subjects.
     */
    public List<String> getAllSubjects() {
        return toJavaList(kschemaRegistry.getAllSubjects());
    }

    /**
     * Get the ID for a specified subject and schema.
     *
     * @param subject The schema subject.
     * @param schema  The parsed schema.
     * @return a list with all subjects.
     */
    public int getId(String subject, ParsedSchema schema) {
        return kschemaRegistry.getId(subject, schema);
    }

    /**
     * Delete all versions of the schema registered under a subject.
     *
     * @param subject The schema subject to delete.
     * @return a list with all versions deleted.
     */
    public List<Integer> deleteSubject(String subject) {
        return toJavaList(kschemaRegistry.deleteSubject(subject));
    }

    /**
     * Delete all versions of the schema registered under a subject.
     *
     * @param subject           The schema subject to delete.
     * @param requestProperties Properties to set for the request.
     * @return a list with all versions deleted.
     */
    public List<Integer> deleteSubject(String subject, Map<String, String> requestProperties) {
        return toJavaList(kschemaRegistry.deleteSubject(subject, toScalaMap(requestProperties)));
    }

    /**
     * Delete a version of the schema registered under a subject.
     *
     * @param subject The schema subject to delete.
     * @param version The version to delete.
     * @return the ID of the deleted version.
     */
    public int deleteSchemaVersion(String subject, String version) {
        return kschemaRegistry.deleteSchemaVersion(subject, version);
    }

    /**
     * Delete a version of the schema registered under a subject.
     *
     * @param subject           The schema subject to delete.
     * @param version           The version to delete.
     * @param requestProperties Properties to set for the request.
     * @return the ID of the deleted version.
     */
    public int deleteSchemaVersion(Map<String, String> requestProperties, String subject, String version) {
        return kschemaRegistry.deleteSchemaVersion(subject, version, toScalaMap(requestProperties));
    }

    /**
     * Reset all content in the cache.
     */
    public void reset() {
        kschemaRegistry.reset();
    }
}
