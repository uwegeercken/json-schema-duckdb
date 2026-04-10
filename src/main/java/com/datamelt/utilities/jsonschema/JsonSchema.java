package com.datamelt.utilities.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class JsonSchema
{
    private final JsonNode rootSchema;

    public JsonSchema(String schemaFileName) throws IOException, IllegalArgumentException
    {
        this(Path.of(schemaFileName).toAbsolutePath().normalize());
    }

    public JsonSchema(Path schemaFilePath) throws IOException, IllegalArgumentException
    {
        ObjectMapper mapper = new ObjectMapper();
        boolean schemaFilePathOk = Files.exists(schemaFilePath) && Files.isRegularFile(schemaFilePath) && Files.isReadable(schemaFilePath);
        if (schemaFilePathOk)
        {
            rootSchema = mapper.readTree(schemaFilePath.toFile());
            if(!validateSchemaContent()) {
                throw new IllegalArgumentException(String.format("the schema file [%s] has no top-level 'properties' — not a valid object schema", schemaFilePath));
            }
        }
        else
        {
            throw new IllegalArgumentException(String.format("the schema file [%s] was not found or is not readable", schemaFilePath));
        }
    }

    private boolean validateSchemaContent() {
        return rootSchema.has("properties") && !rootSchema.get("properties").isEmpty();
    }

    public JsonNode resolveRef(String ref) {
        if (!ref.startsWith("#/")) return null;
        String[] parts = ref.substring(2).split("/");
        JsonNode node  = rootSchema;
        for (String part : parts) {
            part = part.replace("~1", "/").replace("~0", "~");
            node = node.get(part);
            if (node == null) return null;
        }
        return node;
    }

    public boolean has(String ref)
    {
        return rootSchema.has(ref);
    }

    public JsonNode getProperties()
    {
        return rootSchema.get("properties");
    }

    public JsonNode getRequired()
    {
        return rootSchema.get("required");
    }

    public List<String> getRequiredFields()
    {
        List<String> requiredFields = new ArrayList<>();
        JsonNode requiredNode = getRequired();
        if (requiredNode != null && requiredNode.isArray()) {
            requiredNode.forEach(n -> requiredFields.add(n.asText()));
        }
        return requiredFields;
    }
}
