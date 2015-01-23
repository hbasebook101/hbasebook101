package example;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class CASExample3 {
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");

    HConnection connection = HConnectionManager.createConnection(conf);
    HTableInterface table = connection.getTable("ns:tbl");

    SecureRandom random = new SecureRandom();

    while (true) {
      Get get = new Get(Bytes.toBytes("row"));
      get.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("update_num"));
      get.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col1"));
      get.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col2"));

      Result result = table.get(get);
      byte[] oldUpdateNum = result.getValue(Bytes.toBytes("fam"), Bytes.toBytes("update_num"));
      SortedSet<Long> set1 = deserialize(result.getValue(Bytes.toBytes("fam"), Bytes.toBytes("col1")));
      set1.add(random.nextLong());
      SortedSet<Long> set2 = deserialize(result.getValue(Bytes.toBytes("fam"), Bytes.toBytes("col2")));
      set2.add(random.nextLong());

      Put put = new Put(Bytes.toBytes("row"));
      put.add(Bytes.toBytes("fam"), Bytes.toBytes("update_num"), Bytes.incrementBytes(oldUpdateNum, 1));
      put.add(Bytes.toBytes("fam"), Bytes.toBytes("col1"), serialize(set1));
      put.add(Bytes.toBytes("fam"), Bytes.toBytes("col2"), serialize(set2));
      if (table.checkAndPut(Bytes.toBytes("row"), Bytes.toBytes("fam"), Bytes.toBytes("update_num"), oldUpdateNum, put)) {
        break;
      }
    }

    table.close();
    
    connection.close();
  }

  private static byte[] serialize(SortedSet<Long> set) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writeValueAsBytes(set);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static SortedSet<Long> deserialize(byte[] bytes) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue(bytes, new TypeReference<SortedSet<Long>>() {
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
