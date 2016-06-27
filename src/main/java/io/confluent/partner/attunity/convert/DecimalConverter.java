package io.confluent.partner.attunity.convert;


import com.google.common.base.MoreObjects;

import java.math.BigDecimal;

public class DecimalConverter implements Converter {
  final int scale;

  public DecimalConverter(int scale) {
    this.scale = scale;
  }

  @Override
  public Object convert(String inputValue) {
    if(null==inputValue||inputValue.isEmpty()){
      return null;
    }

    return new BigDecimal(inputValue).setScale(this.scale);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("scale", this.scale)
        .toString();
  }
}
