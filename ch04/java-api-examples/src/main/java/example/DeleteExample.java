package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTableInterface table = new HTable(configuration, "ns:tbl");

    Delete delete = new Delete(Bytes.toBytes("row1"));
    delete.deleteColumn(Bytes.toBytes("fam"), Bytes.toBytes("col1"));
    delete.deleteColumn(Bytes.toBytes("fam"), Bytes.toBytes("col2"), 100L);
    table.delete(delete);

    table.close();
  }
}
