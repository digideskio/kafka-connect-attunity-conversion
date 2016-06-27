package io.confluent.partner.attunity;

import io.confluent.partner.attunity.convert.Converter;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public class SchemaValue {
  public final Schema keySchema;
  public final Schema valueSchema;
  public final String topic;
  public final Map<String, Converter> fieldToConverterLookup;


  SchemaValue(Schema keySchema, Schema valueSchema, String topic, Map<String, Converter> fieldToConverterLookup) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.topic = topic;
    this.fieldToConverterLookup = fieldToConverterLookup;
  }
}
