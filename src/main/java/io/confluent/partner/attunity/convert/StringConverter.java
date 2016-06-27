package io.confluent.partner.attunity.convert;

import com.google.common.base.MoreObjects;

public class StringConverter implements Converter {
  @Override
  public Object convert(String inputValue) {
    return inputValue;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .toString();
  }
}
