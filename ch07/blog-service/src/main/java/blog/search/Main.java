package blog.search;

import java.io.IOException;
import java.util.List;

public class Main {
  public static void main(String[] args) throws IOException {
    BlogSearchService blogSearchService = new BlogSearchServiceImpl();
    List<SearchResult> results = blogSearchService.search(1L, "bb");
    for (SearchResult result : results) {
      System.out.println("articeId=" + result.getArticleId() + ", offsets=" + result.getOffsets());
    }
  }
}
