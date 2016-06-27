package io.confluent.partner.attunity.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FieldConfig extends AbstractConfig {
  final String fieldName;

  public FieldConfig(String fieldName, ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
    this.fieldName = fieldName;
  }

  public FieldConfig(String fieldName, Map<String, String> parsedConfig) {
    this(fieldName, conf(), parsedConfig);
  }

  public static final String TYPE_CONFIG = "type";
  private static final String TYPE_DOC = "The type of column. See connect types.";

  public static final String SCALE_CONFIG = "scale";
  private static final String SCALE_DOC = "The scale for a decimal";

  public static final String PRECISION_CONFIG = "precision";
  private static final String PRECISION_DOC = "The precision for a decimal";

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TYPE_DOC)
        .define(SCALE_CONFIG, ConfigDef.Type.INT, Integer.MAX_VALUE, ConfigDef.Range.atLeast(0), ConfigDef.Importance.HIGH, SCALE_DOC)
        .define(PRECISION_CONFIG, ConfigDef.Type.INT, Integer.MAX_VALUE, ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH, PRECISION_DOC);
  }

  public String name() {
    return this.fieldName;
  }

  public String type() {
    return this.getString(TYPE_CONFIG);
  }

  public int scale() {
    return this.getInt(SCALE_CONFIG);
  }

  public int precision() {
    return this.getInt(PRECISION_CONFIG);
  }

}