package blog;

public class Article {
  // ブログ記事ID
  private Long articleId;

  // タイトル
  private String title;

  // 本文
  private String content;

  // カテゴリID
  private Integer categoryId;

  // カテゴリ名
  private String categoryName;

  // 投稿日時
  private Long postAt;

  // 更新日時
  private Long updateAt;

  // ユーザID
  private Long userId;
  
  public Long getArticleId() {
    return articleId;
  }

  public void setArticleId(Long articleId) {
    this.articleId = articleId;
  }

  public Integer getCategoryId() {
    return categoryId;
  }

  public void setCategoryId(Integer categoryId) {
    this.categoryId = categoryId;
  }

  public String getCategoryName() {
    return categoryName;
  }

  public void setCategoryName(String categoryName) {
    this.categoryName = categoryName;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public Long getPostAt() {
    return postAt;
  }

  public void setPostAt(Long postAt) {
    this.postAt = postAt;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public Long getUpdateAt() {
    return updateAt;
  }

  public void setUpdateAt(Long updateAt) {
    this.updateAt = updateAt;
  }
  
  public Long getUserId() {
    return userId;
  }
  
  public void setUserId(Long userId) {
    this.userId = userId;
  }
}
