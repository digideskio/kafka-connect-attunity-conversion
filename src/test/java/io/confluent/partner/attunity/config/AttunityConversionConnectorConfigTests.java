package io.confluent.partner.attunity.config;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttunityConversionConnectorConfigTests {

  @Test
  public void foo() {
    Map<String, String> parsedConfig = new HashMap<>();
    parsedConfig.put("kafka.bootstrap.servers", "confluent:9092");
    parsedConfig.put("kafka.group.id", "testing");
    parsedConfig.put("attunity.input.topic", "input");
    parsedConfig.put("input.0.schema", "NAVDEMO");
    parsedConfig.put("input.0.table", "CUSTOMER");
    parsedConfig.put("input.0.kafka.topic", "connect_navdemo_customer");
    parsedConfig.put("input.0.keys", "C_CUSTKEY");
    parsedConfig.put("input.0.fields.C_CUSTKEY.type", "decimal");
    parsedConfig.put("input.0.fields.C_CUSTKEY.precision", "10");
    parsedConfig.put("input.0.fields.C_CUSTKEY.scale", "0");
    parsedConfig.put("input.0.fields.C_NAME.type", "string");
    parsedConfig.put("input.0.fields.C_ADDRESS.type", "string");
    parsedConfig.put("input.0.fields.C_NATIONKEY.type", "decimal");
    parsedConfig.put("input.0.fields.C_NATIONKEY.precision", "10");
    parsedConfig.put("input.0.fields.C_NATIONKEY.scale", "0");
    parsedConfig.put("input.0.fields.C_PHONE.type", "string");
    parsedConfig.put("input.0.fields.C_ACCTBAL.type", "decimal");
    parsedConfig.put("input.0.fields.C_ACCTBAL.precision", "38");
    parsedConfig.put("input.0.fields.C_ACCTBAL.scale", "10");
    parsedConfig.put("input.0.fields.C_MKTSEGMENT.type", "string");
    parsedConfig.put("input.0.fields.C_COMMENT.type", "string");


    AttunityConversionConnectorConfig config = new AttunityConversionConnectorConfig(parsedConfig);
    List<SourceConfig> sourceConfigs = config.getSourceConfigs();
    for(SourceConfig sourceConfig:sourceConfigs){
      List<FieldConfig> fieldConfigs = sourceConfig.getFieldConfigs();
      for(FieldConfig fieldConfig:fieldConfigs){
        fieldConfig.logUnused();
      }
    }
    System.out.println(sourceConfigs.size());
  }

}
