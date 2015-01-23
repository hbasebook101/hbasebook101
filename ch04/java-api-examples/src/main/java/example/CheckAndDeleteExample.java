package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class CheckAndDeleteExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTable table = new HTable(configuration, "ns:tbl");

    byte[] row = Bytes.toBytes("row1");
    byte[] fam = Bytes.toBytes("fam");
    byte[] col = Bytes.toBytes("col");

    Delete delete = new Delete(row);

    boolean result = table.checkAndDelete(row, fam, col, Bytes.toBytes("value1"), delete);

    System.out.println(result);

    table.close();
  }
}
