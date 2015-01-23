package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class PrefixFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");

    HConnection connection = HConnectionManager.createConnection(conf);
    HTableInterface table = connection.getTable("ns:tbl");

    Scan scan = new Scan();
    scan.setFilter(new PrefixFilter(Bytes.toBytes("prefix")));

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      System.out.println(result);
    }

    table.close();

    connection.close();
  }
}
