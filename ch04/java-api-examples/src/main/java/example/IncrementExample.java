package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class IncrementExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTableInterface table = new HTable(configuration, "ns:tbl");

    Increment increment = new Increment(Bytes.toBytes("row1"));
    increment.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col1"), 1);
    increment.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col2"), 2);

    Result result = table.increment(increment);

    System.out.println(result);

    table.close();
  }
}
