package io.confluent.partner.attunity;

import com.fasterxml.jackson.databind.JsonNode;

public class SchemaHelper {
  public static String getSchema(JsonNode inputValue) {
    JsonNode schemaNode = inputValue.get("schema");
    return schemaNode.asText();
  }

  public static String getTable(JsonNode inputValue) {
    JsonNode tableNode = inputValue.get("table");
    return tableNode.asText();
  }

  public static SchemaKey getSchemaKey(JsonNode inputValue) {
    String schema = getSchema(inputValue);
    String table = getTable(inputValue);
    return new SchemaKey(schema, table);
  }
}
