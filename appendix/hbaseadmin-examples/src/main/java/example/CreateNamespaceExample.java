package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateNamespaceExample {
  public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration configuration = HBaseConfiguration.create();
    
    HBaseAdmin admin = new HBaseAdmin(configuration);
    
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("ns").build();
    admin.createNamespace(namespaceDescriptor);
    
    admin.close();
  }
}
