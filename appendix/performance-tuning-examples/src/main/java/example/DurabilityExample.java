package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class DurabilityExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTableInterface table = new HTable(configuration, "ns:tbl");

    Put put = new Put(Bytes.toBytes("row1"));
    put.add(Bytes.toBytes("fam"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
    put.add(Bytes.toBytes("fam"), Bytes.toBytes("col2"), 100L, Bytes.toBytes("value1"));
    
    put.setDurability(Durability.ASYNC_WAL);
    
    table.put(put);

    table.close();
  }
}
