package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutBufferingExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTableInterface table = new HTable(configuration, "ns:tbl");

    table.setAutoFlush(false, true);

    // 複数のHTableのputメソッドの呼び出し
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.add(Bytes.toBytes("fam"), Bytes.toBytes("col"), Bytes.toBytes("value1"));
    table.put(put1);

    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.add(Bytes.toBytes("fam"), Bytes.toBytes("col"), Bytes.toBytes("value2"));
    table.put(put2);

    table.flushCommits();

    table.close();
  }
}
