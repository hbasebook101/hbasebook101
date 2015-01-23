package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CheckAndPutExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTable table = new HTable(configuration, "ns:tbl");

    byte[] row = Bytes.toBytes("row1");
    byte[] fam = Bytes.toBytes("fam");
    byte[] col = Bytes.toBytes("col");

    Put put = new Put(row);
    put.add(fam, col, Bytes.toBytes("value2"));

    boolean result = table.checkAndPut(row, fam, col, Bytes.toBytes("value1"), put);

    System.out.println(result);

    table.close();
  }
}
