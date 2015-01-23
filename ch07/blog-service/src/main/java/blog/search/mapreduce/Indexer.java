package blog.search.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class Indexer {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();

    Job job = new Job(conf, "blog search");
    job.setJarByClass(Indexer.class);

    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("d"));
    TableMapReduceUtil.initTableMapperJob("ns:blog", scan, IndexMapper.class, ImmutableBytesWritable.class, IntWritable.class, job);
    TableMapReduceUtil.initTableReducerJob("ns:blogsearch", IndexReducer.class, job);
    TableMapReduceUtil.addDependencyJars(job);

    int result = job.waitForCompletion(true) ? 0 : 1;
    System.exit(result);
  }
}
