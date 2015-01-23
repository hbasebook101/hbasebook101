package mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class ColumnCounterReducer extends TableReducer<ImmutableBytesWritable, LongWritable, NullWritable> {

  @Override
  protected void reduce(ImmutableBytesWritable column, Iterable<LongWritable> values, Context context) throws IOException,
      InterruptedException {

    long count = 0;
    for (LongWritable value : values) {
      count += value.get();
    }
    
    Put put = new Put(column.get());
    put.add(Bytes.toBytes("fam"), HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes(count));

    context.write(NullWritable.get(), put);
  }
}
