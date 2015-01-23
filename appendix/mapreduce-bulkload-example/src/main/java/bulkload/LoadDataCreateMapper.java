package bulkload;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class LoadDataCreateMapper extends TableMapper<ImmutableBytesWritable, Put> {

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
    long colValue = Bytes.toLong(value.getValue(Bytes.toBytes("fam"), Bytes.toBytes("col")));
    colValue += 10;

    Put put = new Put(key.get());
    put.add(Bytes.toBytes("fam"), Bytes.toBytes("col"), Bytes.toBytes(colValue));

    context.write(new ImmutableBytesWritable(put.getRow()), put);
  }
}
