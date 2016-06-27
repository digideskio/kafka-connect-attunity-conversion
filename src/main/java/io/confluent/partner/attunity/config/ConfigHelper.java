package io.confluent.partner.attunity.config;

import org.apache.kafka.common.config.AbstractConfig;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ConfigHelper {

  public static Set<String> getPrefixes(AbstractConfig config, String pattern){
    return getPrefixes(config, Pattern.compile(pattern));
  }

  public static Set<String> getPrefixes(AbstractConfig config, Pattern pattern){
    Set<String> prefixes = new LinkedHashSet<>();

    Map<String, String> inputs = config.originalsStrings();
    for(Map.Entry<String, String> kvp:inputs.entrySet()){
      Matcher match = pattern.matcher(kvp.getKey());
      if(match.find()){
        prefixes.add(match.group(0));
      }
    }
    return prefixes;
  }

  public static Map<String, String> originalStringsWithPrefix(AbstractConfig config, String prefix){
    Map<String, Object> originals = config.originalsWithPrefix(prefix);
    Map<String, String> results = new LinkedHashMap<>();

    for(Map.Entry<String, Object> kvp:originals.entrySet()){
      results.put(kvp.getKey(), kvp.getValue()==null?null:kvp.getValue().toString());
    }

    return results;
  }
}
