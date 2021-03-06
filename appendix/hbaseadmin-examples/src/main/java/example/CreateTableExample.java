package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateTableExample {
  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration configuration = HBaseConfiguration.create();
    
    HBaseAdmin admin = new HBaseAdmin(configuration);
    
    // Namespace名が"ns"、Table名が"tbl"
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("ns","tbl"));
    
    // ColumnFamily名が"fam"
    HColumnDescriptor columnDescriptor = new HColumnDescriptor("fam");
    
    // ColumnFamilyを追加
    tableDescriptor.addFamily(columnDescriptor);
    
    // Tableの作成
    admin.createTable(tableDescriptor);
    
    // HBaseAdminのクローズ
    admin.close();
  }
}
