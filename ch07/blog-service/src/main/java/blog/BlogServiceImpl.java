package blog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

public class BlogServiceImpl implements BlogService {
  private final AtomicLong idGenerator = new AtomicLong();
  
  private final HConnection connection;

  private final Hash hash;

  private final Struct blogRowSchema;

  private final Struct secondaryIndexQualifierSchema;

  public BlogServiceImpl() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
    connection = HConnectionManager.createConnection(configuration);

    hash = Hash.getInstance(Hash.MURMUR_HASH3);

    blogRowSchema = new StructBuilder().add(new RawInteger()).add(new RawLong()).toStruct();

    secondaryIndexQualifierSchema = new StructBuilder().add(new RawInteger()).add(new RawLong()).toStruct();
  }

  @Override
  public void postArticle(long userId, String title, String content, int categoryId)
      throws IOException {
    long postAt = System.currentTimeMillis();
    long updateAt = postAt;
    String cagegoryName = getCategoryName(categoryId);
    long articleId = createArticleId();

    byte[] row = createBlogRow(userId);
    Put put = new Put(row);

    put.add(Bytes.toBytes("d"), Bytes.toBytes(Long.MAX_VALUE - articleId),
      serialize(title, content, categoryId, cagegoryName, postAt, updateAt));

    put.add(Bytes.toBytes("s"), createSecondaryIndexQualifier(categoryId, articleId), serialize(title, postAt));

    try (HTableInterface table = connection.getTable("ns:blog")) {
      table.put(put);
    }
  }

  @Override
  public void updateArticle(long userId, long articleId, String newTitle, String newContent, int newCategoryId) throws IOException {
    byte[] row = createBlogRow(userId);

    try (HTableInterface table = connection.getTable("ns:blog")) {
      // 現在のブログ記事を取得
      Get get = new Get(row);
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes(Long.MAX_VALUE - articleId));
      Result result = table.get(get);
      if (result.isEmpty()) {
        return;
      }
      Article article = deserialize(result.value());
      String newCategoryName = article.getCategoryId() == newCategoryId ? article.getCategoryName() : getCategoryName(newCategoryId);
      long updateAt = System.currentTimeMillis();

      RowMutations rowMutations = new RowMutations(row);

      // ブログ記事スキーマのColumnを更新
      Put put = new Put(row);
      put.add(Bytes.toBytes("d"), Bytes.toBytes(Long.MAX_VALUE - articleId),
        serialize(newTitle, newContent, newCategoryId, newCategoryName,
        article.getPostAt(), updateAt));

      // セカンダリインデックススキーマのColumnを追加／更新
      put.add(Bytes.toBytes("s"), createSecondaryIndexQualifier(newCategoryId, articleId), serialize(newTitle, article.getPostAt()));

      rowMutations.add(put);

      // 古いセカンダリインデックスのColumnの削除
      if (article.getCategoryId() != newCategoryId) {
        Delete delete = new Delete(row);
        delete.deleteColumns(Bytes.toBytes("s"), createSecondaryIndexQualifier(article.getCategoryId(), articleId));
        rowMutations.add(delete);
      }

      table.mutateRow(rowMutations);
    }
  }

  @Override
  public void deleteArticle(long userId, long articleId) throws IOException {
    byte[] row = createBlogRow(userId);

    try (HTableInterface table = connection.getTable("ns:blog")) {
      Get get = new Get(row);
      get.addColumn(Bytes.toBytes("d"), Bytes.toBytes(Long.MAX_VALUE - articleId));
      Result result = table.get(get);
      if (result.isEmpty()) {
        return;
      }
      Article article = deserialize(result.value());

      Delete delete = new Delete(row);
      delete.deleteColumns(Bytes.toBytes("d"), Bytes.toBytes(Long.MAX_VALUE - articleId));
      delete.deleteColumns(Bytes.toBytes("s"), createSecondaryIndexQualifier(article.getCategoryId(), articleId));

      table.delete(delete);
    }
  }

  @Override
  public Article getArticle(long userId, long articleId) throws IOException {
    byte[] row = createBlogRow(userId);
    Get get = new Get(row);
    get.addColumn(Bytes.toBytes("d"), Bytes.toBytes(Long.MAX_VALUE - articleId));

    try (HTableInterface table = connection.getTable("ns:blog")) {
      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }
      Article article = deserialize(result.value());
      article.setArticleId(articleId);
      article.setUserId(userId);
      return article;
    }
  }

  @Override
  public List<Article> getArticles(long userId, Long lastArticleId, int length) throws IOException {
    byte[] row = createBlogRow(userId);
    Get get = new Get(row);
    get.addFamily(Bytes.toBytes("d"));
    if (lastArticleId == null) {
      get.setFilter(new ColumnPaginationFilter(length, 0));
    } else {
      get.setFilter(new ColumnPaginationFilter(length, Bytes.toBytes(Long.MAX_VALUE - lastArticleId + 1)));
    }

    try (HTableInterface table = connection.getTable("ns:blog")) {
      Result result = table.get(get);

      List<Article> ret = new ArrayList<>();
      for (Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes("d")).entrySet()) {
        long articleId = Long.MAX_VALUE - Bytes.toLong(entry.getKey());
        byte[] value = entry.getValue();
        Article article = deserialize(value);
        article.setArticleId(articleId);
        article.setUserId(userId);
        ret.add(article);
      }
      return ret;
    }
  }

  @Override
  public List<Article> getArticles(long userId, int categoryId, Long lastArticleId, int length)
      throws IOException {
    byte[] row = createBlogRow(userId);
    Get get = new Get(row);
    get.addFamily(Bytes.toBytes("s"));
    if (lastArticleId == null) {
      get.setFilter(new ColumnPaginationFilter(length, Bytes.toBytes(categoryId)));
    } else {
      get.setFilter(new ColumnPaginationFilter(length,
          incrementBytes(createSecondaryIndexQualifier(categoryId, lastArticleId))));
    }

    try (HTableInterface table = connection.getTable("ns:blog")) {
      Result result = table.get(get);

      List<Article> ret = new ArrayList<>();
      for (Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes("s")).entrySet()) {
        long reversedArticleId =
            (Long) secondaryIndexQualifierSchema.decode(
              new SimplePositionedByteRange(entry.getKey()), 1);
        byte[] value = entry.getValue();
        Article article = deserialize(value);
        article.setArticleId(Long.MAX_VALUE - reversedArticleId);
        article.setUserId(userId);
        article.setCategoryId(categoryId);
        ret.add(article);
      }
      return ret;
    }
  }

  // ブログ記事IDを生成する(ダミー)
  private long createArticleId() throws IOException {
    return idGenerator.getAndIncrement();
  }

  private byte[] createSecondaryIndexQualifier(int categoryId, long articleId) {
    Object[] values = new Object[] { categoryId, Long.MAX_VALUE - articleId };
    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(secondaryIndexQualifierSchema.encodedLength(values));
    secondaryIndexQualifierSchema.encode(positionedByteRange, values);

    return positionedByteRange.getBytes();
  }

  // RowKeyの作成。hash(userId)-userId
  private byte[] createBlogRow(long userId) {
    Object[] value = new Object[] { hash.hash(Bytes.toBytes(userId)), userId };

    SimplePositionedByteRange positionedByteRange =
        new SimplePositionedByteRange(blogRowSchema.encodedLength(value));
    blogRowSchema.encode(positionedByteRange, value);

    return positionedByteRange.getBytes();
  }

  // 各データをbyte[]にシリアライズする
  public byte[] serialize(String title, String content, int categoryId,
      String categoryName, long postAt, long updateAt) {
    Article article = new Article();
    article.setTitle(title);
    article.setContent(content);
    article.setCategoryId(categoryId);
    article.setCategoryName(categoryName);
    article.setPostAt(postAt);
    article.setUpdateAt(updateAt);

    ObjectMapper objectMapper = new ObjectMapper();
    SerializationConfig config = objectMapper.getSerializationConfig();
    config.setSerializationInclusion(Inclusion.NON_NULL);

    try {
      return objectMapper.writeValueAsBytes(article);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] serialize(String title, long postAt) {
    Article article = new Article();
    article.setTitle(title);
    article.setPostAt(postAt);

    ObjectMapper objectMapper = new ObjectMapper();
    SerializationConfig config = objectMapper.getSerializationConfig();
    config.setSerializationInclusion(Inclusion.NON_NULL);

    try {
      return objectMapper.writeValueAsBytes(article);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // byte[]からArticleにデシリアライズする
  private Article deserialize(byte[] bytes) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.readValue(bytes, Article.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // カテゴリ名の取得(ダミー。受け取ったcategoryIdをStringに変換してそのまま返している)
  private String getCategoryName(int categoryId) {
    return Integer.toString(categoryId);
  }

  private static byte[] incrementBytes(final byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      boolean increase = false;

      final int val = bytes[bytes.length - (i + 1)] & 0x0ff;
      int total = val + 1;
      if (total > 255) {
        increase = true;
        total = 0;
      }
      bytes[bytes.length - (i + 1)] = (byte) total;
      if (!increase) {
        return bytes;
      }
    }
    return bytes;
  }
}
