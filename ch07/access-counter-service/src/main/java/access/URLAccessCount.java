package access;

import java.util.Calendar;

public class URLAccessCount {
  // ドメイン
  private String domain;
  
  // パス
  private String path;
  
  // 日時 or 日付
  private Calendar time;
  
  // アクセス数
  private long count;

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Calendar getTime() {
    return time;
  }

  public void setTime(Calendar time) {
    this.time = time;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  @Override
  public String toString() {
    return "URLAccessCount [count=" + getCount() + ", domain=" + getDomain() + ", path=" + path + ", time=" + getTime() + "]";
  }
}
