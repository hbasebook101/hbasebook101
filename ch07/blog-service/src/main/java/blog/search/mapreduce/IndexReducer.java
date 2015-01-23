package blog.search.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.codehaus.jackson.map.ObjectMapper;

public class IndexReducer extends TableReducer<ImmutableBytesWritable, IntWritable, NullWritable> {
  private final Struct blogSearchIndexRowSchema;

  private final Hash hash;

  public IndexReducer() {
    hash = Hash.getInstance(Hash.MURMUR_HASH3);

    blogSearchIndexRowSchema = new StructBuilder().add(new RawInteger()).add(new RawLong()).toStruct();
  }
  
  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    ByteBuffer buffer = ByteBuffer.wrap(key.get());
    long userId = buffer.getLong();
    long articleId = buffer.getLong();
    byte[] word = new byte[buffer.capacity() - buffer.position()];
    buffer.get(word);

    SortedSet<Integer> offsets = new TreeSet<>();
    for (IntWritable value : values) {
      offsets.add(value.get());
    }

    byte[] row = createBlogSearchIndexRow(userId);
    Put put = new Put(row);
    put.add(Bytes.toBytes("d"), word, articleId, serialize(offsets));

    context.write(NullWritable.get(), put);
  }

  private byte[] createBlogSearchIndexRow(long userId) {
    Object[] values = new Object[] { hash.hash(Bytes.toBytes(userId)), userId };
    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(blogSearchIndexRowSchema.encodedLength(values));
    blogSearchIndexRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

  private byte[] serialize(Collection<Integer> offsets) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsBytes(offsets);
  }
}
