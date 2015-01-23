package messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.RawString;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

public class MessagingServiceImpl implements MessagingService {

  private final HConnection connection;

  private final Hash hash;

  private final Struct messageRowSchema;

  public MessagingServiceImpl() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
    connection = HConnectionManager.createConnection(configuration);

    hash = Hash.getInstance(Hash.MURMUR_HASH3);

    messageRowSchema = new StructBuilder().add(new RawInteger()).add(new RawLong()).add(new RawLong()).add(RawString.ASCENDING).toStruct();
  }

  @Override
  public void sendMessage(long roomId, long userId, String body) throws IOException {
    long postAt = System.currentTimeMillis();
    String messageId = UUID.randomUUID().toString();
    byte[] row = createMessageRow(roomId, postAt, messageId);

    Put put = new Put(row);
    put.add(Bytes.toBytes("m"), Bytes.toBytes("messageId"), Bytes.toBytes(messageId));
    put.add(Bytes.toBytes("m"), Bytes.toBytes("userId"), Bytes.toBytes(userId));
    put.add(Bytes.toBytes("m"), Bytes.toBytes("body"), Bytes.toBytes(body));

    try (HTableInterface table = connection.getTable("ns:message")) {
      table.put(put);
    }
  }

  @Override
  public List<Message> getInitialMessages(long roomId, List<Long> blockUsers) throws IOException {
    byte[] startRow = createMessageScanRow(roomId);
    byte[] stopRow = incrementBytes(createMessageScanRow(roomId));

    Scan scan = new Scan(startRow, stopRow);

    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

    if (blockUsers != null) {
      for (Long userId : blockUsers) {
        SingleColumnValueFilter userFilter =
            new SingleColumnValueFilter(Bytes.toBytes("m"), Bytes.toBytes("userId"),
                CompareOp.NOT_EQUAL, Bytes.toBytes(userId));
        filterList.addFilter(userFilter);
      }
    }

    scan.setFilter(filterList);

    List<Message> messages = new ArrayList<>();
    try (HTableInterface table = connection.getTable("ns:message");
        ResultScanner scanner = table.getScanner(scan);) {
      int count = 0;
      for (Result result : scanner) {
        messages.add(convertToMessage(result));
        count++;
        if (count >= 50) {
          break;
        }
      }
    }

    return messages;
  }

  @Override
  public List<Message> getOldMessages(long roomId, Message lastMessage, List<Long> blockUsers)
      throws IOException {
    byte[] startRow =
        incrementBytes(createMessageRow(roomId, lastMessage.getPostAt(),
          lastMessage.getMessageId()));
    byte[] stopRow = incrementBytes(createMessageScanRow(roomId));

    Scan scan = new Scan(startRow, stopRow);

    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

    if (blockUsers != null) {
      for (Long userId : blockUsers) {
        SingleColumnValueFilter userFilter =
            new SingleColumnValueFilter(Bytes.toBytes("m"), Bytes.toBytes("userId"),
                CompareOp.NOT_EQUAL, Bytes.toBytes(userId));
        filterList.addFilter(userFilter);
      }
    }

    scan.setFilter(filterList);

    List<Message> messages = new ArrayList<>();
    try (HTableInterface table = connection.getTable("ns:message");
        ResultScanner scanner = table.getScanner(scan);) {
      int count = 0;
      for (Result result : scanner) {
        messages.add(convertToMessage(result));
        count++;
        if (count >= 50) {
          break;
        }
      }
    }

    return messages;
  }

  @Override
  public List<Message> getNewMessages(long roomId, Message stopMessage, List<Long> blockUsers)
      throws IOException {
    byte[] startRow = createMessageScanRow(roomId);
    byte[] stopRow = createMessageRow(roomId, stopMessage.getPostAt(), stopMessage.getMessageId());

    Scan scan = new Scan(startRow, stopRow);

    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

    if (blockUsers != null) {
      for (Long userId : blockUsers) {
        SingleColumnValueFilter userFilter =
            new SingleColumnValueFilter(Bytes.toBytes("m"), Bytes.toBytes("userId"),
                CompareOp.NOT_EQUAL, Bytes.toBytes(userId));
        filterList.addFilter(userFilter);
      }
    }

    scan.setFilter(filterList);

    List<Message> messages = new ArrayList<>();
    try (HTableInterface table = connection.getTable("ns:message");
        ResultScanner scanner = table.getScanner(scan);) {
      int count = 0;
      for (Result result : scanner) {
        messages.add(convertToMessage(result));
        count++;
        if (count >= 50) {
          break;
        }
      }
    }

    return messages;
  }


  private Message convertToMessage(Result result) {
    Message message = new Message();
    message.setUserId(Bytes.toLong(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("userId"))));
    message.setBody(Bytes.toString(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("body"))));
    message.setMessageId(Bytes.toString(result.getValue(Bytes.toBytes("m"), Bytes.toBytes("messageId"))));
    message.setPostAt(Long.MAX_VALUE - (long) messageRowSchema.decode(new SimplePositionedByteRange(result.getRow()), 1));

    return message;
  }

  private byte[] createMessageRow(long roomId, long postAt, String messageId) {
    Object[] values = new Object[] { hash.hash(Bytes.toBytes(roomId)), roomId, Long.MAX_VALUE - postAt, messageId };

    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(messageRowSchema.encodedLength(values));
    messageRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

  private byte[] createMessageScanRow(long roomId) {
    Object[] values = new Object[] { hash.hash(Bytes.toBytes(roomId)), roomId };

    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(messageRowSchema.encodedLength(values));
    messageRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
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
