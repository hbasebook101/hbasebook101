package blog.search;

import java.util.List;

public class SearchResult {
  // ブログ記事ID
  private long articleId;
  
  // 出現位置のリスト
  private List<Integer> offsets;
  
  public long getArticleId() {
    return articleId;
  }
  public void setArticleId(long articleId) {
    this.articleId = articleId;
  }
  public List<Integer> getOffsets() {
    return offsets;
  }
  public void setOffsets(List<Integer> offsets) {
    this.offsets = offsets;
  }
}
