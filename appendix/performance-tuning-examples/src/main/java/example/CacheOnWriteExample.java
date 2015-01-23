package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CacheOnWriteExample {

  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration configuration = HBaseConfiguration.create();

    HBaseAdmin admin = new HBaseAdmin(configuration);
    HTableDescriptor tableDesc = new HTableDescriptor("ns:tbl");
    HColumnDescriptor cfDesc = new HColumnDescriptor("fam");

    // HFileに書き込む際にインデックスのブロックをキャッシュするようにする
    cfDesc.setCacheIndexesOnWrite(true);

    // HFileに書き込む際にブルームフィルタのブロックをキャッシュするようにする
    cfDesc.setCacheBloomsOnWrite(true);

    // HFileに書き込む際にデータのブロックをキャッシュするようにする
    cfDesc.setCacheDataOnWrite(true);

    tableDesc.addFamily(cfDesc);

    admin.createTable(tableDesc);

    admin.close();
  }
}
