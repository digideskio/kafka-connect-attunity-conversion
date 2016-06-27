package io.confluent.partner.attunity.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class AttunityConversionConnectorConfig extends AbstractConfig {

  public static final String ATTUNITY_TOPIC_CONFIG = "attunity.input.topic";
  private static final String ATTUNITY_TOPIC_DOC = "The input topic for this task. This is the target topic for the Attunity configuration.";





  public AttunityConversionConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public AttunityConversionConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define("kafka." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Type.STRING, Importance.HIGH, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
        .define("kafka." + ConsumerConfig.GROUP_ID_CONFIG, Type.STRING, Importance.HIGH, ConsumerConfig.GROUP_ID_CONFIG)
        .define(ATTUNITY_TOPIC_CONFIG, Type.STRING, Importance.HIGH, ATTUNITY_TOPIC_DOC);
  }

  public String attunityTopic(){
    return this.getString(ATTUNITY_TOPIC_CONFIG);
  }

  public Properties kafkaProperties(){
    Properties properties = new Properties();
    Map<String, Object> value = this.originalsWithPrefix("kafka.");
    properties.putAll(value);
    return properties;
  }

  public List<SourceConfig> getSourceConfigs() {
    List<SourceConfig> results = new ArrayList<>();
    Set<String> prefixes = ConfigHelper.getPrefixes(this, "input\\.\\d+\\.");

    for(String prefix:prefixes){
      Map<String, String> originals = ConfigHelper.originalStringsWithPrefix(this, prefix);

      SourceConfig sourceConfig = new SourceConfig(originals);
      results.add(sourceConfig);
    }

    return results;
  }




}
