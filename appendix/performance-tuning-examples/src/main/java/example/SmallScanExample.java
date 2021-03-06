package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class SmallScanExample {

  @SuppressWarnings("resource")
  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTable table = new HTable(configuration, "ns:tbl");

    Scan scan = new Scan(Bytes.toBytes("row1"), Bytes.toBytes("row3"));
    scan.setSmall(true);

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      System.out.println(result);
    }
  }
}
