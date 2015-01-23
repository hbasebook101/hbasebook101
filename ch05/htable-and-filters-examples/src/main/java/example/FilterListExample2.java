package example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterListExample2 {
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");

    HConnection connection = HConnectionManager.createConnection(conf);
    HTableInterface table = connection.getTable("ns:tbl");

    List<Filter> filters = new ArrayList<>();
    filters.add(new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("col"))));
    filters.add(new ValueFilter(CompareOp.LESS, new BinaryComparator(Bytes.toBytes("value"))));
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);

    Scan scan = new Scan();
    scan.setFilter(filterList);

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      System.out.println(result);
    }

    table.close();

    connection.close();
  }
}
