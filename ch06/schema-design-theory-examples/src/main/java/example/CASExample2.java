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

public class CASExample2 {
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");

    HConnection connection = HConnectionManager.createConnection(conf);
    HTableInterface table = connection.getTable("ns:tbl");

    SecureRandom random = new SecureRandom();

    while (true) {
      Get get = new Get(Bytes.toBytes("row"));
      get.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col"));

      Result result = table.get(get);
      byte[] OldValue = result.getValue(Bytes.toBytes("fam"), Bytes.toBytes("col"));

      SortedSet<Long> set = deserialize(OldValue);
      set.add(random.nextLong());

      Put put = new Put(Bytes.toBytes("row"));
      put.add(Bytes.toBytes("fam"), Bytes.toBytes("col"), serialize(set));

      if (table.checkAndPut(Bytes.toBytes("row"), Bytes.toBytes("fam"), Bytes.toBytes("col"), OldValue, put)) {
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