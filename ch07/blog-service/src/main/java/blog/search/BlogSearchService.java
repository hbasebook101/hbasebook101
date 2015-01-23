package blog.search;

import java.io.IOException;
import java.util.List;

public interface BlogSearchService {
  //ブログを検索する
  List<SearchResult> search(long userId, String word) throws IOException;
}
