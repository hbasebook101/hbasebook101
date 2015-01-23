package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

public class ColumnCounter {
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Job job = new Job(conf, "ColumnCounter");
    job.setJarByClass(ColumnCounter.class);

    Scan scan = new Scan();

    TableMapReduceUtil.initTableMapperJob("ns:columncount", scan, ColumnCounterMapper.class, ImmutableBytesWritable.class, LongWritable.class, job);

    TableMapReduceUtil.initTableReducerJob("ns:countresult", ColumnCounterReducer.class, job);

    int status = job.waitForCompletion(true) ? 0 : 1;
    System.exit(status);
  }
}
