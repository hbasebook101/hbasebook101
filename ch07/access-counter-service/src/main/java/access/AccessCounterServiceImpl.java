package access;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawString;
import org.apache.hadoop.hbase.types.RawStringTerminated;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

public class AccessCounterServiceImpl implements AccessCounterService {

  private final HConnection connection;

  private final Hash hash;

  private final Struct urlRowSchema;

  private final Struct domainRowSchema;

  public AccessCounterServiceImpl() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
    connection = HConnectionManager.createConnection(configuration);

    hash = Hash.getInstance(Hash.MURMUR_HASH3);

    urlRowSchema = new StructBuilder().add(new RawInteger()).add(new RawStringTerminated(new byte[] { 0x00 }))
            .add(RawString.ASCENDING).toStruct();

    domainRowSchema = new StructBuilder().add(new RawInteger()).add(RawString.ASCENDING).toStruct();
  }

  @Override
  public void count(String subDomain, String rootDomain, String path, int amount)
      throws IOException {
    String domain = subDomain + "." + rootDomain;
    String reversedDomain = reverseDomain(domain);

    Date date = new Date();
    SimpleDateFormat hourlyFormat = new SimpleDateFormat("yyyyMMddHH");
    SimpleDateFormat dailyFormat = new SimpleDateFormat("yyyyMMddHH");

    List<Row> increments = new ArrayList<>();

    // URL
    byte[] urlRow = createURLRow(domain, path);
    Increment urlIncrement = new Increment(urlRow);
    urlIncrement.addColumn(Bytes.toBytes("h"), Bytes.toBytes(hourlyFormat.format(date)), amount);
    urlIncrement.addColumn(Bytes.toBytes("d"), Bytes.toBytes(dailyFormat.format(date)), amount);
    increments.add(urlIncrement);

    // ドメイン
    byte[] domainRow = createDomainRow(rootDomain, reversedDomain);
    Increment domainIncrement = new Increment(domainRow);
    domainIncrement.addColumn(Bytes.toBytes("h"), Bytes.toBytes(hourlyFormat.format(date)), amount);
    domainIncrement.addColumn(Bytes.toBytes("d"), Bytes.toBytes(dailyFormat.format(date)), amount);
    increments.add(domainIncrement);

    try (HTableInterface table = connection.getTable("ns:access")) {
      Object[] results = new Object[2];
      table.batch(increments, results);
    } catch (InterruptedException e) {
      // エラー
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<URLAccessCount> getHourlyURLAccessCount(String subDomain, String rootDomain,
      String path, Calendar startHour, Calendar endHour) throws IOException {
    String domain = subDomain + "." + rootDomain;
    byte[] row = createURLRow(domain, path);
    Get get = new Get(row);
    get.addFamily(Bytes.toBytes("h"));

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
    get.setFilter(new ColumnRangeFilter(Bytes.toBytes(sdf.format(startHour.getTime())), true, Bytes
        .toBytes(sdf.format(endHour.getTime())), true));

    try (HTableInterface table = connection.getTable("ns:access")) {
      List<URLAccessCount> ret = new ArrayList<URLAccessCount>();

      Result result = table.get(get);
      if (result.isEmpty()) {
        return ret;
      }

      for (Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes("h")).entrySet()) {
        byte[] yyyyMMddHH = entry.getKey();
        byte[] value = entry.getValue();

        URLAccessCount accessCount = new URLAccessCount();

        Calendar time = Calendar.getInstance();
        time.setTime(sdf.parse(Bytes.toString(yyyyMMddHH)));

        accessCount.setTime(time);
        accessCount.setDomain(domain);
        accessCount.setPath(path);
        accessCount.setCount(Bytes.toLong(value));

        ret.add(accessCount);
      }

      return ret;
    } catch (ParseException e) {
      // エラー
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<URLAccessCount> getDailyURLAccessCount(String subDomain, String rootDomain,
      String path, Calendar startDay, Calendar endDay) throws IOException {
    String domain = subDomain + "." + rootDomain;
    byte[] row = createURLRow(domain, path);
    Get get = new Get(row);
    get.addFamily(Bytes.toBytes("d"));

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    get.setFilter(new ColumnRangeFilter(Bytes.toBytes(sdf.format(startDay.getTime())), true, Bytes.toBytes(sdf.format(endDay.getTime())), true));

    try (HTableInterface table = connection.getTable("ns:access")) {
      List<URLAccessCount> ret = new ArrayList<URLAccessCount>();

      Result result = table.get(get);
      if (result.isEmpty()) {
        return ret;
      }

      for (Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes("d")).entrySet()) {
        byte[] yyyyMMdd = entry.getKey();
        byte[] value = entry.getValue();

        URLAccessCount accessCount = new URLAccessCount();

        Calendar time = Calendar.getInstance();
        time.setTime(sdf.parse(Bytes.toString(yyyyMMdd)));

        accessCount.setTime(time);
        accessCount.setDomain(domain);
        accessCount.setPath(path);
        accessCount.setCount(Bytes.toLong(value));

        ret.add(accessCount);
      }

      return ret;
    } catch (ParseException e) {
      // エラー
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<DomainAccessCount> getHourlyDomainAccessCount(String rootDomain, Calendar startHour,
      Calendar endHour) throws IOException {
    String reversedRootDomain = reverseDomain(rootDomain);
    byte[] startRow = createDomainRow(rootDomain, reversedRootDomain);
    byte[] stopRow = incrementBytes(createDomainRow(rootDomain, reversedRootDomain));

    Scan scan = new Scan(startRow, stopRow);
    scan.addFamily(Bytes.toBytes("h"));

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");

    scan.setFilter(new ColumnRangeFilter(Bytes.toBytes(sdf.format(startHour.getTime())), true,
        Bytes.toBytes(sdf.format(endHour.getTime())), true));

    try (HTableInterface table = connection.getTable("ns:access"); ResultScanner scanner = table.getScanner(scan)) {

      List<DomainAccessCount> ret = new ArrayList<DomainAccessCount>();

      for (Result result : scanner) {
        String domain = reverseDomain((String) domainRowSchema.decode(new SimplePositionedByteRange(result.getRow()), 1));

        for (Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes("h")).entrySet()) {
          byte[] yyyyMMddHH = entry.getKey();
          byte[] value = entry.getValue();

          DomainAccessCount accessCount = new DomainAccessCount();

          Calendar time = Calendar.getInstance();
          time.setTime(sdf.parse(Bytes.toString(yyyyMMddHH)));

          accessCount.setTime(time);
          accessCount.setDomain(reverseDomain(domain));
          accessCount.setCount(Bytes.toLong(value));

          ret.add(accessCount);
        }
      }

      return ret;
    } catch (ParseException e) {
      // エラー
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<DomainAccessCount> getDailyDomainAccessCount(String rootDomain, Calendar startDay,
      Calendar endDay) throws IOException {
    String reversedRootDomain = reverseDomain(rootDomain);
    byte[] startRow = createDomainRow(rootDomain, reversedRootDomain);
    byte[] stopRow = incrementBytes(createDomainRow(rootDomain, reversedRootDomain));
    Scan scan = new Scan(startRow, stopRow);
    scan.addFamily(Bytes.toBytes("d"));

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    scan.setFilter(new ColumnRangeFilter(Bytes.toBytes(sdf.format(startDay.getTime())), true, Bytes.toBytes(sdf.format(endDay.getTime())), true));

    try (HTableInterface table = connection.getTable("ns:access"); ResultScanner scanner = table.getScanner(scan)) {

      List<DomainAccessCount> ret = new ArrayList<DomainAccessCount>();

      for (Result result : scanner) {
        String domain = reverseDomain((String) domainRowSchema.decode(new SimplePositionedByteRange(result.getRow()), 1));

        for (Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes("d")).entrySet()) {
          byte[] qualifier = entry.getKey();
          byte[] value = entry.getValue();

          DomainAccessCount accessCount = new DomainAccessCount();

          Calendar time = Calendar.getInstance();
          time.setTime(sdf.parse(Bytes.toString(qualifier)));

          accessCount.setTime(time);
          accessCount.setDomain(reverseDomain(domain));
          accessCount.setCount(Bytes.toLong(value));

          ret.add(accessCount);
        }
      }

      return ret;
    } catch (ParseException e) {
      // エラー
      throw new RuntimeException(e);
    }
  }

  private byte[] createDomainRow(String rootDomain, String reversedDomain) {
    Object[] values = new Object[] { hash.hash(Bytes.toBytes(rootDomain)), reversedDomain };
    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(domainRowSchema.encodedLength(values));
    domainRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

  private byte[] createURLRow(String domain, String path) {
    Object[] values = new Object[] { hash.hash(Bytes.toBytes(domain)), domain, path };
    SimplePositionedByteRange positionedByteRange = new SimplePositionedByteRange(urlRowSchema.encodedLength(values));
    urlRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

  private String reverseDomain(String domain) {
    String[] split = domain.split("\\.", 2);
    if (split.length == 1) {
      return domain;
    }
    return reverseDomain(split[1]) + "." + split[0];
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
