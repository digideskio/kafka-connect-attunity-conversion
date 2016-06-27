package io.confluent.partner.attunity.convert;

import org.apache.kafka.connect.data.Date;

public class DateConverter implements Converter {
  @Override
  public Object convert(String inputValue) {
    if(null==inputValue||inputValue.isEmpty()){
      return null;
    }
    // Take a
    // look at org.apache.kafka.connect.data.Date.
    //TODO:Add patterns to parse the date data from attunity.
    throw new UnsupportedOperationException("Add patterns to parse the timestamp data from Attunity.");
  }
}
