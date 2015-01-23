package blog.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class BlogSearchServiceImpl implements BlogSearchService {

  private final HConnection connection;

  private final Struct blogSearchIndexRowSchema;

  private final Hash hash;

  public BlogSearchServiceImpl() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
    connection = HConnectionManager.createConnection(configuration);

    blogSearchIndexRowSchema = new StructBuilder().add(new RawInteger()).add(new RawLong()).toStruct();
    
    hash = Hash.getInstance(Hash.MURMUR_HASH3);
  }

  @Override
  public List<SearchResult> search(long userId, String word) throws IOException {
    byte[] row = createBlogSearchIndexRow(userId);

    Get get = new Get(row);
    get.addColumn(Bytes.toBytes("d"), Bytes.toBytes(word));
    get.setMaxVersions();

    try (HTableInterface table = connection.getTable("ns:blogsearch")) {
      List<SearchResult> searchResults = new ArrayList<>();

      Result result = table.get(get);
      if (result.isEmpty()) {
        return searchResults;
      }

      for (Entry<Long, byte[]> entry : result.getMap().get(Bytes.toBytes("d")).get(Bytes.toBytes(word)).entrySet()) {
        SearchResult searchResult = new SearchResult();
        searchResult.setArticleId(entry.getKey());
        searchResult.setOffsets(deserializeOffsets(entry.getValue()));
        searchResults.add(searchResult);
      }
      return searchResults;
    }
  }

  public byte[] createBlogSearchIndexRow(long userId) {

    Object[] values = new Object[] { hash.hash(Bytes.toBytes(userId)), userId };
    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(blogSearchIndexRowSchema.encodedLength(values));
    blogSearchIndexRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

  private List<Integer> deserializeOffsets(byte[] offsetsBytes) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(offsetsBytes, new TypeReference<List<Integer>>() {
    });
  }
}
