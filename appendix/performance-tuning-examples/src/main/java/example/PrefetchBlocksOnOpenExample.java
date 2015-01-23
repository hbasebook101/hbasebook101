package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class PrefetchBlocksOnOpenExample {

  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration configuration = HBaseConfiguration.create();

    HBaseAdmin admin = new HBaseAdmin(configuration);
    HTableDescriptor tableDesc = new HTableDescriptor("ns:tbl");
    HColumnDescriptor cfDesc = new HColumnDescriptor("fam");

    // ブロックキャッシュのウォーミングアップを有効にする
    cfDesc.setPrefetchBlocksOnOpen(true);

    tableDesc.addFamily(cfDesc);

    admin.createTable(tableDesc);

    admin.close();
  }
}
