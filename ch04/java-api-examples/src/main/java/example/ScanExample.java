package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanExample {
  
  @SuppressWarnings("unused")
  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTableInterface table = new HTable(configuration, "ns:tbl");

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes("row1"));
    scan.setStopRow(Bytes.toBytes("row3"));
    scan.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col1"));
    scan.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col2"));
    
    ResultScanner scanner = table.getScanner(scan);
    
    for (Result result : scanner) {
      byte[] col1 = result.getValue(Bytes.toBytes("fam"), Bytes.toBytes("col1"));
      byte[] col2 = result.getValue(Bytes.toBytes("fam"), Bytes.toBytes("col2"));
    }

    table.close();
  }
}
