/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.latestn;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "LATESTN", description = LatestNValues.DESCRIPTION)
public final class LatestNValues {

  static final String DESCRIPTION = "This function returns the n most recent vallues";
  static final String SEQ_FIELD = "SEQ";
  static final String VAL_FIELD = "VAL";

  private LatestNValues() {}

  public static final Schema STRUCT_INTEGER =
      SchemaBuilder.struct().optional().field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
          .field(VAL_FIELD, Schema.OPTIONAL_INT32_SCHEMA).build();

  public static final Schema STRUCT_LONG =
      SchemaBuilder.struct().optional().field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
          .field(VAL_FIELD, Schema.OPTIONAL_INT64_SCHEMA).build();

  public static final Schema STRUCT_DOUBLE =
      SchemaBuilder.struct().optional().field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
          .field(VAL_FIELD, Schema.OPTIONAL_FLOAT64_SCHEMA).build();

  public static final Schema STRUCT_BOOLEAN =
      SchemaBuilder.struct().optional().field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
          .field(VAL_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA).build();

  public static final Schema STRUCT_STRING =
      SchemaBuilder.struct().optional().field(SEQ_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
          .field(VAL_FIELD, Schema.OPTIONAL_STRING_SCHEMA).build();

  static AtomicLong sequence = new AtomicLong();

  static <T> Struct createStruct(final Schema schema, final T val) {
    final Struct struct = new Struct(schema);
    struct.put(SEQ_FIELD, generateSequence());
    struct.put(VAL_FIELD, val);
    return struct;
  }

  private static long generateSequence() {
    return sequence.getAndIncrement();
  }

  private static int compareStructs(final Struct struct1, final Struct struct2) {
    // Deal with overflow - we assume if one is positive and the other negative then the sequence
    // has overflowed - in which case the latest is the one with the smallest sequence
    final long sequence1 = struct1.getInt64(SEQ_FIELD);
    final long sequence2 = struct2.getInt64(SEQ_FIELD);
    if (sequence1 < 0 && sequence2 >= 0) {
      return 1;
    } else if (sequence2 < 0 && sequence1 >= 0) {
      return -1;
    } else {
      return Long.compare(sequence1, sequence2);
    }
  }

  @UdafFactory(description = "return the latest value of an integer column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL INT>>")
  public static Udaf<Integer, List<Struct>, List<Integer>> latestInteger(final int topNSize) {
    return latest(STRUCT_INTEGER, topNSize);
  }

  @UdafFactory(description = "return the latest value of an big integer column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BIGINT>>")
  public static Udaf<Long, List<Struct>, List<Long>> latestLong(final int topNSize) {
    return latest(STRUCT_LONG, topNSize);
  }

  @UdafFactory(description = "return the latest value of a double column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL DOUBLE>>")
  public static Udaf<Double, List<Struct>, List<Double>> latestDouble(final int topNSize) {
    return latest(STRUCT_DOUBLE, topNSize);
  }

  @UdafFactory(description = "return the latest value of a boolean column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BOOLEAN>>")
  public static Udaf<Boolean, List<Struct>, List<Boolean>> latestBoolean(final int topNSize) {
    return latest(STRUCT_BOOLEAN, topNSize);
  }

  @UdafFactory(description = "return the latest value of a string column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL STRING>>")
  public static Udaf<String, List<Struct>, List<String>> latestString(final int topNSize) {
    return latest(STRUCT_STRING, topNSize);
  }

  static final Comparator<Struct> structComparator = new Comparator<Struct>() {

    @Override
    public int compare(final Struct struct1, final Struct struct2) {
      final long sequence1 = struct1.getInt64(SEQ_FIELD);
      final long sequence2 = struct2.getInt64(SEQ_FIELD);
      if (sequence1 < 0 && sequence2 >= 0) {
        return 1;
      } else if (sequence2 < 0 && sequence1 >= 0) {
        return -1;
      } else {
        return Long.compare(sequence1, sequence2);
      }
    }
  };

  @UdafFactory(description = "Latest by offset")
  static <T> Udaf<T, List<Struct>, List<T>> latest(final Schema structSchema, final int topNSize) {
    return new Udaf<T, List<Struct>, List<T>>() {
      @Override
      public List<Struct> initialize() {
        return new ArrayList<Struct>();
      }

      @Override
      public List<Struct> aggregate(final T current, final List<Struct> aggregate) {
        if (current == null) {
          return aggregate;
        }

        aggregate.add(createStruct(structSchema, current));
        final int currentSize = aggregate.size();
        if (currentSize <= topNSize) {
          return aggregate;
        } else {
          return aggregate.subList(currentSize - topNSize, currentSize);
        }
      }

      @Override
      public List<Struct> merge(final List<Struct> aggOne, final List<Struct> aggTwo) {
        final List<Struct> merged = new ArrayList<>();
        merged.addAll(aggOne);
        merged.addAll(aggTwo);
        final int currentSize = merged.size();
        Collections.sort(merged, structComparator);
        if (currentSize <= topNSize) {
          return merged;
        } else {
          return merged.subList(currentSize - topNSize, currentSize);
        }
      }

      @Override
      @SuppressWarnings("unchecked")
      public List<T> map(final List<Struct> agg) {
        return (List<T>) agg.stream().map(s -> s.get(VAL_FIELD)).collect(Collectors.toList());
      }
    };
  }

}
