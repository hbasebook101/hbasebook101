package bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class LoadDataCreater {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();

    Job job = new Job(conf, "bulkload sample");
    job.setJarByClass(LoadDataCreater.class);

    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col"));

    TableMapReduceUtil.initTableMapperJob("ns:bulkload", scan, LoadDataCreateMapper.class, ImmutableBytesWritable.class, Put.class, job);
    HFileOutputFormat2.setOutputPath(job, new Path("/tmp/loadfiles"));
    HFileOutputFormat2.configureIncrementalLoad(job, new HTable(conf, "ns:bulkload"));

    int status =  job.waitForCompletion(true) ? 0 : 1;
    System.exit(status);
  }
}
