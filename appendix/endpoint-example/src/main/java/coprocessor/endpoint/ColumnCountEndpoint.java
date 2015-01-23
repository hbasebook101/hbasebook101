package coprocessor.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import coprocessor.endpoint.ExampleEndpoints.ColumnCountRequest;
import coprocessor.endpoint.ExampleEndpoints.ColumnCountResponse;

public class ColumnCountEndpoint extends ExampleEndpoints.ColumnCountService implements Coprocessor, CoprocessorService {
  private RegionCoprocessorEnvironment env;

  public void start(CoprocessorEnvironment env) throws IOException {
    this.env = (RegionCoprocessorEnvironment) env;
  }

  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  public Service getService() {
    return this;
  }

  public void getColumnCount(RpcController controller, ColumnCountRequest request, RpcCallback<ColumnCountResponse> done) {
    Scan scan = new Scan();

    ColumnCountResponse response = null;

    try (InternalScanner scanner = env.getRegion().getScanner(scan)) {
      long count = 0;

      List<Cell> results = new ArrayList<>();
      boolean hasNext;
      do {
        hasNext = scanner.next(results);
        count += results.size();
        results.clear();
      } while (hasNext);

      response = ColumnCountResponse.newBuilder().setCount(count).build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }

    done.run(response);
  }
}