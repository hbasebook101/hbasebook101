package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTableInterface table = new HTable(configuration, "ns:tbl");

    Get get = new Get(Bytes.toBytes("row1"));
    get.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col1"));
    get.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col2"));

    Result result = table.get(get);

    byte[] col1 = result.getValue(Bytes.toBytes("fam"), Bytes.toBytes("col1"));
    byte[] col2 = result.getValue(Bytes.toBytes("fam"), Bytes.toBytes("col2"));

    System.out.println(Bytes.toStringBinary(col1));
    System.out.println(Bytes.toStringBinary(col2));
    
    table.close();
  }
}
