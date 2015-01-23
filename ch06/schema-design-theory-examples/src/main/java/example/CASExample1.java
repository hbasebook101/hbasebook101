package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CASExample1 {
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");

    HConnection connection = HConnectionManager.createConnection(conf);
    HTableInterface table = connection.getTable("ns:tbl");

    Put put = new Put(Bytes.toBytes("row"));
    put.add(Bytes.toBytes("fam"), Bytes.toBytes("col"), Bytes.toBytes("valueA"));

    boolean success = table.checkAndPut(Bytes.toBytes("row"), Bytes.toBytes("fam"), Bytes.toBytes("col"), HConstants.EMPTY_BYTE_ARRAY, put);

    System.out.println(success);

    table.close();

    connection.close();
  }
}
