package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ColumnPaginationFilterExample1 {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");

    HConnection connection = HConnectionManager.createConnection(conf);
    HTableInterface table = connection.getTable("ns:tbl");

    Get get = new Get(Bytes.toBytes("row1"));
    get.setFilter(new ColumnPaginationFilter(2, 3));
    Result result = table.get(get);
    
    System.out.println(result);
    
    table.close();
    
    connection.close();
  }
}
