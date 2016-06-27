package io.confluent.partner.attunity.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SourceConfig  extends AbstractConfig {

  public SourceConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public SourceConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static final String SCHEMA_CONFIG = "schema";
  private static final String SCHEMA_DOC = "The input schema.";

  public static final String TABLE_CONFIG = "table";
  private static final String TABLE_DOC = "The input table";

  public static final String KAFKA_TOPIC_CONFIG = "kafka.topic";
  private static final String KAFKA_TOPIC_DOC = "The kafka topic to write to";

  public static final String KEYS_CONFIG = "keys";
  private static final String KEYS_DOC = "The columns that are part of the primary key. This will be the key generated to kafka.";

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(SCHEMA_CONFIG, Type.STRING, Importance.HIGH, SCHEMA_DOC)
        .define(TABLE_CONFIG, Type.STRING, Importance.HIGH, TABLE_DOC)
        .define(KAFKA_TOPIC_CONFIG, Type.STRING, Importance.HIGH, KAFKA_TOPIC_DOC)
        .define(KEYS_CONFIG, Type.LIST, Importance.HIGH, KEYS_DOC);
  }

  public List<FieldConfig> getFieldConfigs(){
    List<FieldConfig> results = new ArrayList<>();
    Pattern pattern = Pattern.compile("fields\\.(.+)\\.");

    Set<String> prefixes = ConfigHelper.getPrefixes(this, pattern);

    for(String prefix:prefixes){
      Matcher match = pattern. matcher(prefix);
      if(!match.find()){
        throw new IllegalStateException(
            String.format("Prefix '%s' does not match pattern %s",
                prefix,
                pattern.pattern()
                )
        );
      }
      String fieldName = match.group(1);
      Map<String, String> originals = ConfigHelper.originalStringsWithPrefix(this, prefix);

      FieldConfig sourceConfig = new FieldConfig(fieldName, originals);
      results.add(sourceConfig);
    }

    return results;
  }

  public String schema(){
    return this.getString(SCHEMA_CONFIG);
  }

  public String table(){
    return this.getString(TABLE_CONFIG);
  }

  public String kafkaTopic() {
    return this.getString(KAFKA_TOPIC_CONFIG);
  }

  public List<String> keys() {
    return this.getList(KEYS_CONFIG);
  }
}
