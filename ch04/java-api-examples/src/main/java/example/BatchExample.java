package example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

public class BatchExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration configuration = HBaseConfiguration.create();
    HTable table = new HTable(configuration, "ns:tbl");

    List<Row> actions = new ArrayList<>();
    Put put = new Put(Bytes.toBytes("row1"));
    put.add(Bytes.toBytes("fam"), Bytes.toBytes("col"), Bytes.toBytes("value1"));
    actions.add(put);

    Get get = new Get(Bytes.toBytes("row2"));
    get.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col"));
    actions.add(get);

    Delete delete = new Delete(Bytes.toBytes("row3"));
    delete.deleteColumns(Bytes.toBytes("fam"), Bytes.toBytes("col"));
    actions.add(delete);

    Increment increment = new Increment(Bytes.toBytes("row4"));
    increment.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col"), 1);
    actions.add(increment);

    Append append = new Append(Bytes.toBytes("row5"));
    append.add(Bytes.toBytes("fam"), Bytes.toBytes("col"), Bytes.toBytes("value5"));
    actions.add(append);

    Object[] results = new Object[actions.size()];
    table.batch(actions, results);
    for (Object result : results) {
      if (result instanceof Result) {
        System.out.println(result);
      }
    }

    table.close();
  }
}
