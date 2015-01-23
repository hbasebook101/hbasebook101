package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class GetClusterStatusExample {
  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration configuration = HBaseConfiguration.create();
    
    HBaseAdmin admin = new HBaseAdmin(configuration);
    
    // クラスタの状態を取得
    ClusterStatus clusterStatus = admin.getClusterStatus();
    
    // クラスタの状態を標準出力に出力
    System.out.println(clusterStatus);
    
    // HBaseAdminのクローズ
    admin.close();
  }
}
