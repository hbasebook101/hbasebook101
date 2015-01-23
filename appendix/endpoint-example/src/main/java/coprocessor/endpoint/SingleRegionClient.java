package coprocessor.endpoint;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

import coprocessor.endpoint.ExampleEndpoints.ColumnCountRequest;
import coprocessor.endpoint.ExampleEndpoints.ColumnCountResponse;
import coprocessor.endpoint.ExampleEndpoints.ColumnCountService;

public class SingleRegionClient {
  public static void main(String[] args) throws IOException, ServiceException {
    Configuration configuration = HBaseConfiguration.create();
    HConnection connection = HConnectionManager.createConnection(configuration);

    try (HTableInterface table = connection.getTable("ns:endpoint")) {
      final ColumnCountRequest request = ExampleEndpoints.ColumnCountRequest.newBuilder().build();

      CoprocessorRpcChannel channel = table.coprocessorService(Bytes.toBytes("row1"));
      ColumnCountService.BlockingInterface stub = ColumnCountService.newBlockingStub(channel);
      ColumnCountResponse response = stub.getColumnCount(null, request);

      long count = response.getCount();
      System.out.println(count);
    }
  }
}