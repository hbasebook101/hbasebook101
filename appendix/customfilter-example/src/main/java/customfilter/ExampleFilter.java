package customfilter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleFilter extends FilterBase {
  private boolean filterOutRow;
  private final byte[] rowKeyPrefix;
  private final byte[] columnNamePrefix;

  public ExampleFilter(byte[] rowKeyPrefix, byte[] columnNamePrefix) {
    this.rowKeyPrefix = rowKeyPrefix;
    this.columnNamePrefix = columnNamePrefix;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
    if (length < rowKeyPrefix.length) {
      filterOutRow = true;
    } else {
      filterOutRow = Bytes.compareTo(buffer, offset, rowKeyPrefix.length, rowKeyPrefix, 0, rowKeyPrefix.length) != 0;
    }
    return filterOutRow;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    if (filterOutRow) {
      return ReturnCode.NEXT_ROW;
    }

    int comparisonLength = Math.min(columnNamePrefix.length, v.getQualifierLength());
    int compare = Bytes.compareTo(v.getQualifierArray(), v.getQualifierOffset(), comparisonLength, columnNamePrefix, 0,
        columnNamePrefix.length);

    if (compare < 0) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    if (compare > 0) {
      return ReturnCode.NEXT_ROW;
    }

    return ReturnCode.INCLUDE;
  }

  @Override
  public Cell getNextCellHint(Cell kv) {
    return KeyValue.createFirstOnRow(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getFamilyArray(), kv.getFamilyOffset(),
        kv.getFamilyLength(), columnNamePrefix, 0, columnNamePrefix.length);
  }

  @Override
  public boolean filterRow() throws IOException {
    return filterOutRow;
  }

  @Override
  public void reset() throws IOException {
    filterOutRow = false;
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    return false;
  }

  @Override
  public byte[] toByteArray() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(4 + rowKeyPrefix.length + 4 + columnNamePrefix.length);
    buffer.putInt(rowKeyPrefix.length).put(rowKeyPrefix);
    buffer.putInt(columnNamePrefix.length).put(columnNamePrefix);
    return buffer.array();
  }

  public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
    ByteBuffer buffer = ByteBuffer.wrap(pbBytes);
    byte[] rowKeyPrefix = new byte[buffer.getInt()];
    buffer.get(rowKeyPrefix);
    byte[] columnNamePrefix = new byte[buffer.getInt()];
    buffer.get(columnNamePrefix);

    return new ExampleFilter(rowKeyPrefix, columnNamePrefix);
  }
}