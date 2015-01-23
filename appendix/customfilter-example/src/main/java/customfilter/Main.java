package customfilter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {
  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HConnection connection = HConnectionManager.createConnection(configuration);
    HTableInterface table = connection.getTable("ns:tbl");
    
    Scan scan = new Scan();
    scan.setFilter(new ExampleFilter(Bytes.toBytes("row2"), Bytes.toBytes("col2")));
    
    ResultScanner scanner = table.getScanner(scan);
    for(Result result : scanner) {
      System.out.println(result);
    }
  }
}
