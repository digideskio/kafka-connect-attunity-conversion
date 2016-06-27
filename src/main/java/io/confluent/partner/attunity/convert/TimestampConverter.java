package io.confluent.partner.attunity.convert;

import org.apache.kafka.connect.data.Timestamp;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class TimestampConverter implements Converter {
  @Override
  public Object convert(String inputValue) {
    if(null==inputValue||inputValue.isEmpty()){
      return null;
    }
    // Take a look at org.apache.kafka.connect.data.Timestamp.
    //TODO:Add patterns to parse the timestamp data from attunity.
    throw new UnsupportedOperationException("Add patterns to parse the timestamp data from Attunity.");
  }
}
