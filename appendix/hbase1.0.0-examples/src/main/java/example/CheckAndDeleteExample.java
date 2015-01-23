package example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class CheckAndDeleteExample {
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    Connection connection = ConnectionFactory.createConnection(conf);

    TableName tableName = TableName.valueOf("ns:tbl");
    Table table = connection.getTable(tableName);

    byte[] row = Bytes.toBytes("row1");
    byte[] fam = Bytes.toBytes("fam");
    byte[] col = Bytes.toBytes("col");

    Delete delete = new Delete(row);

    boolean result = table.checkAndDelete(row, fam, col, CompareOp.NOT_EQUAL, Bytes.toBytes("value1"), delete);

    System.out.println(result);

    table.close();

    connection.close();
  }
}
