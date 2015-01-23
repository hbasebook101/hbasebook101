package example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;

public class MutateRowExample {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    HTableInterface table = new HTable(configuration, "ns:tbl");

    byte[] row = Bytes.toBytes("row1");
    Put put = new Put(row);
    put.add(Bytes.toBytes("fam"), Bytes.toBytes("col1"), Bytes.toBytes("value"));

    Delete delete = new Delete(row);
    delete.deleteColumns(Bytes.toBytes("fam"), Bytes.toBytes("col2"));

    RowMutations rowMutations = new RowMutations(row);
    rowMutations.add(put);
    rowMutations.add(delete);

    table.mutateRow(rowMutations);

    table.close();
  }
}
