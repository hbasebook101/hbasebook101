package document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawString;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

public class DocumentVersionManagerImpl implements DocumentVersionManager {

  private final HConnection connection;

  private final Hash hash;

  private final Struct documentRowSchema;

  public DocumentVersionManagerImpl() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
    connection = HConnectionManager.createConnection(configuration);

    hash = Hash.getInstance(Hash.MURMUR_HASH3);

    documentRowSchema = new StructBuilder().add(new RawInteger()).add(RawString.ASCENDING).toStruct();
  }

  @Override
  public void save(String documentId, String title, String text) throws IOException {
    byte[] row = createDocumentRow(documentId);

    try (HTableInterface table = connection.getTable("ns:document")) {
      while (true) {
        Get get = new Get(row);
        get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("ver"));
        Result verResult = table.get(get);
        Long version = null;
        byte[] oldVersionBytes = null;
        if (verResult.isEmpty()) {
          oldVersionBytes = HConstants.EMPTY_BYTE_ARRAY;
          version = 1L;
        } else {
          oldVersionBytes = verResult.getValue(Bytes.toBytes("d"), Bytes.toBytes("ver"));
          long oldVersion = Bytes.toLong(oldVersionBytes);
          version = oldVersion + 1;
        }

        Put put = new Put(row, version);
        put.add(Bytes.toBytes("d"), Bytes.toBytes("ver"), Bytes.toBytes(version));
        put.add(Bytes.toBytes("d"), Bytes.toBytes("title"), Bytes.toBytes(title));
        put.add(Bytes.toBytes("d"), Bytes.toBytes("text"), Bytes.toBytes(text));

        boolean success =
            table.checkAndPut(row, Bytes.toBytes("d"), Bytes.toBytes("ver"), oldVersionBytes, put);
        if (success) {
          return;
        }
      }
    }
  }

  @Override
  public List<Long> listVersions(String documentId) throws IOException {
    byte[] row = createDocumentRow(documentId);
    Get get = new Get(row);
    get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("ver"));
    get.setMaxVersions();

    List<Long> versions = new ArrayList<>();
    try (HTableInterface table = connection.getTable("ns:document")) {
      Result result = table.get(get);
      for (Long version : result.getMap().get(Bytes.toBytes("d")).get(Bytes.toBytes("ver"))
          .keySet()) {
        versions.add(version);
      }
      return versions;
    }
  }

  @Override
  public Document getLatest(String documentId) throws IOException {
    byte[] row = createDocumentRow(documentId);

    try (HTableInterface table = connection.getTable("ns:document")) {
      Get get = new Get(row);
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("ver"));
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("title"));
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("text"));

      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }

      Document document = new Document();
      document.setDocumentId(documentId);
      document.setVersion(Bytes.toLong(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("ver"))));
      document.setTitle(Bytes.toString(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("title"))));
      document.setText(Bytes.toString(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("text"))));

      return document;
    }
  }

  @Override
  public Document get(String documentId, long version) throws IOException {
    byte[] row = createDocumentRow(documentId);

    try (HTableInterface table = connection.getTable("ns:document")) {
      Get get = new Get(row);
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("title"));
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("text"));
      get.setTimeStamp(version);

      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }

      Document document = new Document();
      document.setDocumentId(documentId);
      document.setVersion(version);
      document.setTitle(Bytes.toString(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("title"))));
      document.setText(Bytes.toString(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("text"))));

      return document;
    }
  }

  private byte[] createDocumentRow(String documentId) {
    Object[] values = new Object[] { hash.hash(Bytes.toBytes(documentId)), documentId };
    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(documentRowSchema.encodedLength(values));
    documentRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }
}