package example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;


public class ClientExample {
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    Connection connection = ConnectionFactory.createConnection(conf);

    TableName tableName = TableName.valueOf("ns:tbl");
    Table table = connection.getTable(tableName);

    table.close();

    connection.close();
  }
}
