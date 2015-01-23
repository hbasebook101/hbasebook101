package blog.search.mapreduce;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.hadoop.io.IntWritable;
import org.codehaus.jackson.map.ObjectMapper;

import blog.Article;

public class IndexMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

  private final Struct blogRowSchema;

  public IndexMapper() {
    blogRowSchema = new StructBuilder().add(new RawInteger()).add(new RawLong()).toStruct();
  }

  @Override
  public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
    for (Entry<byte[], byte[]> columnEntry : value.getFamilyMap(Bytes.toBytes("d")).entrySet()) {
      long articleId = Long.MAX_VALUE - Bytes.toLong(columnEntry.getKey());
      byte[] val = columnEntry.getValue();

      long userId = (long) blogRowSchema.decode(new SimplePositionedByteRange(key.get()), 1);

      Article article = deserialize(val);
      String[] splitContent = article.getContent().split(" ");
      for (int i = 0; i < splitContent.length; i++) {
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.add(Bytes.toBytes(userId),
            Bytes.toBytes(articleId), Bytes.toBytes(splitContent[i])));
        context.write(immutableBytesWritable, new IntWritable(i));
      }
    }
  }

  private Article deserialize(byte[] bytes) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.readValue(bytes, Article.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
