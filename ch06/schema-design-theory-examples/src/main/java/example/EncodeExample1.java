package example;

import org.apache.hadoop.hbase.types.OrderedBlob;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

public class EncodeExample1 {

  @SuppressWarnings("unused")
  public static void main(String[] args) {
    byte[] bytes = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };

    SimplePositionedByteRange ascByteRange = new SimplePositionedByteRange(OrderedBlob.ASCENDING.encodedLength(bytes));
    OrderedBlob.ASCENDING.encode(ascByteRange, bytes);
    byte[] ascBytes = ascByteRange.getBytes();

    SimplePositionedByteRange descByteRange = new SimplePositionedByteRange(OrderedBlob.DESCENDING.encodedLength(bytes));
    OrderedBlob.DESCENDING.encode(descByteRange, bytes);
    byte[] descBytes = descByteRange.getBytes();

    int intValue = 100;
    RawInteger rawInteger = new RawInteger();
    SimplePositionedByteRange rawIntByteRange = new SimplePositionedByteRange(rawInteger.encodedLength(intValue));
    rawInteger.encode(rawIntByteRange, intValue);
    byte[] rawIntBytes = rawIntByteRange.getBytes();
  }
}
