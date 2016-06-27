package io.confluent.partner.attunity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {
  static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public JsonNode deserialize(String s, byte[] bytes) {
    if (null == bytes || bytes.length == 0) {
      return null;
    }

    try {
      return this.objectMapper.readValue(bytes, JsonNode.class);
    } catch (IOException e) {
      throw new KafkaException("Exception thrown while deserializing json", e);
    }
  }

  @Override
  public void close() {

  }
}
