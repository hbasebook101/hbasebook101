package example;

import org.apache.hadoop.hbase.types.OrderedInt32;
import org.apache.hadoop.hbase.types.OrderedInt64;
import org.apache.hadoop.hbase.types.RawShort;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

public class EncodeExample2 {

  @SuppressWarnings("unused")
  public static void main(String[] args) {
    Struct struct = new StructBuilder()
                        .add(OrderedInt32.ASCENDING)
                        .add(OrderedInt64.ASCENDING)
                        .add(new RawShort())
                        .toStruct();

    int userId = 10;
    long timestamp = System.currentTimeMillis();
    short age = 30;

    Object[] values = new Object[] { userId, timestamp, age };

    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(struct.encodedLength(values));
    struct.encode(positionedByteRange, values);

    byte[] bytes = positionedByteRange.getBytes();
  }
}
