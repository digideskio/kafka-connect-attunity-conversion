package io.confluent.partner.attunity;

import io.confluent.partner.attunity.config.AttunityConversionConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AttunityConversionConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(AttunityConversionConnector.class);
  Map<String, String> inputConfig;
  private AttunityConversionConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new AttunityConversionConnectorConfig(map);
    this.inputConfig = map;
  }

  @Override
  public Class<? extends Task> taskClass() {
    //TODO: Return your task implementation.
    return AttunityConversionTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    taskConfigs.add(this.inputConfig);
    return taskConfigs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return AttunityConversionConnectorConfig.conf();
  }
}
