package coprocessor.endpoint;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import coprocessor.endpoint.ExampleEndpoints.ColumnCountRequest;
import coprocessor.endpoint.ExampleEndpoints.ColumnCountResponse;
import coprocessor.endpoint.ExampleEndpoints.ColumnCountService;

public class MultiRegionClient {
  public static void main(String[] args) throws Throwable {
    Configuration configuration = HBaseConfiguration.create();
    HConnection connection = HConnectionManager.createConnection(configuration);

    try (HTableInterface table = connection.getTable("ns:endpoint")) {
      final ColumnCountRequest request = ColumnCountRequest.newBuilder().build(); 

      Map<byte[], Long> results = table.coprocessorService(ColumnCountService.class, 
          Bytes.toBytes("row1"), Bytes.toBytes("row4"),
          new Batch.Call<ColumnCountService, Long>() {

            @Override
            public Long call(ColumnCountService columnCounterService) throws IOException {
              BlockingRpcCallback<ColumnCountResponse> columnCounterCallback = new BlockingRpcCallback<>(); 
              columnCounterService.getColumnCount(null, request, columnCounterCallback); 
              ColumnCountResponse columnCountResponse = columnCounterCallback.get(); 
              return columnCountResponse.getCount(); 
            }
          });

      for (Entry<byte[], Long> entry : results.entrySet()) { 
        System.out.println("region=" + Bytes.toStringBinary(entry.getKey()) + ", count=" + entry.getValue());
      }
    }
  }
}