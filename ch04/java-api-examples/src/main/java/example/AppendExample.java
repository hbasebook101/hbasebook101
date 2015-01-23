package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class AppendExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTableInterface table = new HTable(configuration, "ns:tbl");

    Append append = new Append(Bytes.toBytes("row1"));
    append.add(Bytes.toBytes("fam"), Bytes.toBytes("col1"), Bytes.toBytes("appendValue1"));
    append.add(Bytes.toBytes("fam"), Bytes.toBytes("col2"), Bytes.toBytes("appendValue2"));

    Result result = table.append(append);

    System.out.println(result);

    table.close();
  }
}
