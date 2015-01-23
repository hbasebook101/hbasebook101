package access;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Main {
  public static void main(String[] args) throws IOException {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());

    AccessCounterService accessCounterService = new AccessCounterServiceImpl();
    accessCounterService.count("blog", "ameba.jp", "test1", 1);
    accessCounterService.count("blog", "ameba.jp", "test2", 2);
    accessCounterService.count("ranking", "ameba.jp", "test3", 10);
    accessCounterService.count("ranking", "ameba.jp", "test4", 20);
    accessCounterService.count("official", "ameba.jp", "test5", 100);
    accessCounterService.count("official", "ameba.jp", "test6", 200);

    List<URLAccessCount> hourlyTest1Counts = accessCounterService.getHourlyURLAccessCount("blog", "ameba.jp", "test1", calendar, calendar);
    System.out.println("hourlyTest1Counts=" + hourlyTest1Counts);

    List<DomainAccessCount> hourlyAmebaJpCounts = accessCounterService.getHourlyDomainAccessCount("ameba.jp", calendar, calendar);
    System.out.println("hourlyAmebaJpCounts=" + hourlyAmebaJpCounts);

    List<URLAccessCount> dailyTest3Counts = accessCounterService.getDailyURLAccessCount("ranking", "ameba.jp", "test3", calendar, calendar);
    System.out.println("dailyTest3Counts=" + dailyTest3Counts);

    List<DomainAccessCount> dailyAmebaJpCounts = accessCounterService.getDailyDomainAccessCount("ameba.jp", calendar, calendar);
    System.out.println("dailyAmebaJpCounts=" + dailyAmebaJpCounts);
  }
}
