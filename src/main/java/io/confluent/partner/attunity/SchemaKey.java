package io.confluent.partner.attunity;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;


class SchemaKey implements Comparable<SchemaKey> {
  public final String schema;
  public final String table;


  public SchemaKey(String schema, String table) {
    this.schema = schema;
    this.table = table;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.schema, this.table);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("schema", this.schema)
        .add("table", this.table)
        .toString();
  }


  @Override
  public int compareTo(SchemaKey that) {
    if (null == that) {
      return 1;
    }
    return ComparisonChain.start()
        .compare(this.schema, that.schema)
        .compare(this.table, that.table)
        .result();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SchemaKey) {
      return compareTo((SchemaKey) obj) == 0;
    } else {
      return false;
    }
  }
}

