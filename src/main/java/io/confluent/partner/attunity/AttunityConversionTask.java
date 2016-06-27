package io.confluent.partner.attunity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.partner.attunity.config.AttunityConversionConnectorConfig;
import io.confluent.partner.attunity.config.FieldConfig;
import io.confluent.partner.attunity.config.SourceConfig;
import io.confluent.partner.attunity.convert.Converter;
import io.confluent.partner.attunity.convert.DateConverter;
import io.confluent.partner.attunity.convert.DecimalConverter;
import io.confluent.partner.attunity.convert.StringConverter;
import io.confluent.partner.attunity.convert.TimestampConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AttunityConversionTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(AttunityConversionTask.class);
  final JsonNodeDeserializer deserializer = new JsonNodeDeserializer();
  private AttunityConversionConnectorConfig config;
  private Object defaultObject = new Object();
  private KafkaConsumer<JsonNode, JsonNode> consumer;
  private Map<SchemaKey, SchemaValue> schemaLookup = new HashMap<>();
  private Cache<SchemaKey, Object> invalidSchemas = CacheBuilder.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .maximumSize(2000)
      .build();

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  Schema buildValueSchema(SourceConfig sourceConfig, Map<String, Converter> fieldConverters) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {
      Converter converter;
      SchemaBuilder builder;

      switch (fieldConfig.type()) {
        case "string":
          builder = SchemaBuilder.string().optional();
          converter = new StringConverter();
          break;
        case "date":
          builder = Date.builder().optional();
          converter = new DateConverter();
          break;
        case "timestamp":
          builder = Timestamp.builder().optional();
          converter = new TimestampConverter();
          break;
        case "decimal":
          builder = Decimal.builder(fieldConfig.scale()).optional();
          converter = new DecimalConverter(fieldConfig.scale());
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Field '%s' with type '%s' is not supported.", fieldConfig.name(), fieldConfig.type())
          );
      }

      Schema fieldSchema = builder.build();
      schemaBuilder.field(fieldConfig.name(), fieldSchema);
      fieldConverters.put(fieldConfig.name(), converter);
      if(log.isInfoEnabled()){
        log.info("Mapping {}.{}.{} to {} converting with {}", sourceConfig.schema(), sourceConfig.table(), fieldConfig.name(), fieldSchema, converter);
      }
    }

    return schemaBuilder.build();
  }

  Schema buildKeySchema(SourceConfig sourceConfig, Schema valueSchema) {
    SchemaBuilder builder = SchemaBuilder.struct();

    for (String key : sourceConfig.keys()) {
      Field field = valueSchema.field(key);

      if (null == field) {
        throw new IllegalStateException(
            String.format(
                "Schema does not have field '%s'",
                key
            )
        );
      }

      builder.field(key, field.schema());
    }

    return builder.build();
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new AttunityConversionConnectorConfig(map);

    for (SourceConfig sourceConfig : this.config.getSourceConfigs()) {
      Map<String, Converter> fieldConverters = new HashMap<>();
      Schema valueSchema = buildValueSchema(sourceConfig, fieldConverters);
      Schema keySchema = buildKeySchema(sourceConfig, valueSchema);

      SchemaKey schemaKey = new SchemaKey(sourceConfig.schema(), sourceConfig.table());
      SchemaValue schemaValue = new SchemaValue(keySchema, valueSchema, sourceConfig.kafkaTopic(), fieldConverters);
      schemaLookup.put(schemaKey, schemaValue);
      if (log.isInfoEnabled()) {
        log.info("Setup conversion for {}", schemaKey);
      }
    }

    if (log.isInfoEnabled()) {
      log.info("Setting up the kafka consumer.");
    }
    this.consumer = new KafkaConsumer<>(this.config.kafkaProperties(), deserializer, deserializer);
    List<String> topics = new ArrayList<>();
    if (log.isInfoEnabled()) {
      log.info("Subscribing to topic '{}'", this.config.attunityTopic());
    }
    topics.add(this.config.attunityTopic());
    this.consumer.subscribe(topics);
  }

  void convert(Struct record, final Map<String, Converter> fieldToConverterLookup, Map<String, String> jsonData) {
    for (Field field : record.schema().fields()) {
      String inputValue = jsonData.get(field.name());
      Object value;
      if (null == inputValue) {
        value = null;
      } else {
        Converter converter = fieldToConverterLookup.get(field.name());
        value = converter.convert(inputValue);
      }
      record.put(field.name(), value);
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {

    List<SourceRecord> results = new ArrayList<>();

    ConsumerRecords<JsonNode, JsonNode> records = this.consumer.poll(1000L);

    if (records.isEmpty()) {
      return results;
    }

    for (ConsumerRecord<JsonNode, JsonNode> record : records) {
      try {
        SchemaKey schemaKey = SchemaHelper.getSchemaKey(record.value());
        SchemaValue schemaValue = this.schemaLookup.get(schemaKey);

        //Do we have this schema configured? If not log it once per 10 minutes and move on
        if (null == schemaValue) {
          if (null == invalidSchemas.getIfPresent(schemaKey) && log.isErrorEnabled()) {
            log.error("Schema is not configured. {}", schemaKey);
            invalidSchemas.put(schemaKey, defaultObject);
          }
          continue;
        }

        //Attunity json is funky. Normalize that funk into a single row.
        JsonNode dataNode = record.value().get("data");
        Map<String, String> jsonData = jsonDataToMap(dataNode);

        Struct keyStruct = new Struct(schemaValue.keySchema);
        Struct valueStruct = new Struct(schemaValue.valueSchema);

        convert(keyStruct, schemaValue.fieldToConverterLookup, jsonData);
        convert(valueStruct, schemaValue.fieldToConverterLookup, jsonData);

        SourceRecord sourceRecord = new SourceRecord(null, null, schemaValue.topic, schemaValue.keySchema, keyStruct, schemaValue.valueSchema, valueStruct);
        results.add(sourceRecord);
      } catch (Exception ex) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown", ex);
        }
      }
    }

    return results;
  }

  Map<String, String> jsonDataToMap(JsonNode dataNode) throws JsonProcessingException {
    Map<String, String> result = new HashMap<>();
    Iterator<JsonNode> dataChildren = dataNode.elements();
    while (dataChildren.hasNext()) {
      JsonNode fieldNode = dataChildren.next();
      Map<String, String> fieldResult = JsonNodeDeserializer.objectMapper.treeToValue(fieldNode, Map.class);
      result.putAll(fieldResult);
    }
    return result;
  }


  @Override
  public void stop() {
    this.consumer.close();
  }
}