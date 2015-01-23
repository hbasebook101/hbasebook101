package example;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;

public class ToBytesExample {

  @SuppressWarnings("unused")
  public static void main(String[] args) {
    // 文字列をバイト配列に変換する
    byte[] byteArrayFromString = Bytes.toBytes("row");
    // バイト配列を文字列に変換する
    String stringFromByteArray = Bytes.toString(byteArrayFromString);
    // boolean型をバイト配列に変換する
    byte[] byteArrayFromBoolean = Bytes.toBytes(true);
    // バイト配列をboolean型に変換する
    boolean booleanFromByteArray = Bytes.toBoolean(byteArrayFromBoolean);
    // int型をバイト配列に変換する
    byte[] byteArrayFromInt = Bytes.toBytes(1);
    // バイト配列をint型に変換する
    int intFromByteArray = Bytes.toInt(byteArrayFromInt);
    // float型をバイト配列に変換する
    byte[] byteArrayFromFloat = Bytes.toBytes(1.1);
    // バイト配列をfloat型に変換する
    float floatFromByteArray = Bytes.toFloat(byteArrayFromFloat);
    // BigDecimalクラスをバイト配列に変換する
    byte[] byteArrayFromBigDecimal = Bytes.toBytes(new BigDecimal(100));
    // バイト配列をBigDecimalクラスに変換する
    BigDecimal bigDecimalFromByteArray = Bytes.toBigDecimal(byteArrayFromBigDecimal);
  }
}
