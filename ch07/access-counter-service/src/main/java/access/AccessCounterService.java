package access;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

public interface AccessCounterService {
  // アクセスをカウントする
  void count(String subDomain, String rootDomain, String path, int amount) throws IOException;

  // URLのアクセスカウントをアワリーの粒度で取得する
  List<URLAccessCount> getHourlyURLAccessCount(String subDomain, String rootDomain, String path,
      Calendar startHour, Calendar endHour) throws IOException;

  // URLのアクセスカウントをデイリーの粒度で取得する
  List<URLAccessCount> getDailyURLAccessCount(String subDomain, String rootDomain, String path,
      Calendar startDay, Calendar endDay) throws IOException;

  // ルートドメインの全てのサブドメインのアクセスカウントをアワリーの粒度で取得する
  List<DomainAccessCount> getHourlyDomainAccessCount(String rootDomain, Calendar startHour,
      Calendar endHour) throws IOException;

  // ルートドメインの全てのサブドメインのアクセスカウントをデイリーの粒度で取得する
  List<DomainAccessCount> getDailyDomainAccessCount(String rootDomain, Calendar startDay,
      Calendar endDay) throws IOException;
}
