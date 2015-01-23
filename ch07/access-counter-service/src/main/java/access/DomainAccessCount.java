package access;

import java.util.Calendar;

public class DomainAccessCount {
  // ドメイン
  private String domain;

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
    return "DomainAccessCount [count=" + count + ", domain=" + domain + ", time=" + time + "]";
  }
}
