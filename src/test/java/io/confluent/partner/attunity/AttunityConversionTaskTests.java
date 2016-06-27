package io.confluent.partner.attunity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.partner.attunity.config.AttunityConversionConnectorConfig;
import io.confluent.partner.attunity.config.SourceConfig;
import io.confluent.partner.attunity.convert.Converter;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttunityConversionTaskTests {
  AttunityConversionTask task;

  @Before
  public void before(){
    this.task  = new AttunityConversionTask();
  }

  @Test
  public void test(){
    AttunityConversionTask task = new AttunityConversionTask();
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
    SourceConfig sourceConfig = sourceConfigs.get(0);

    Map<String, Converter> fieldConverters = new HashMap<>();
    Schema schema = task.buildValueSchema(sourceConfig, fieldConverters);







  }

  @Test
  public void jsonDataToMap() throws IOException {
    final String input ="{\"data\":[{\"C_CUSTKEY\":\"6\"},{\"C_NAME\":\"Chuck Rush\"},{\"C_ADDRESS\":\"6 Rodeo Drive\"},{\"C_NATIONKEY\":\"20\"},{\"C_PHONE\":\"30-114-968-4951\"},{\"C_ACCTBAL\":\"13830.5700000000\"},{\"C_MKTSEGMENT\":\"AUTOMOBILE\"},{\"C_COMMENT\":\"Had a previous account\"}]}";
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readValue(input, JsonNode.class);
    JsonNode dataNode = jsonNode.get("data");

    Map<String, String> normalizedData=this.task.jsonDataToMap(dataNode);
    System.out.println(normalizedData.get("C_NAME"));





  }


}
