package blog;

import java.io.IOException;
import java.util.List;

public interface BlogService {
  // ブログ記事投稿
  void postArticle(long userId, String title, String content, int categoryId) throws IOException;

  // ブログ記事更新
  void updateArticle(long userId, long articleId, String newTitle, String newContent, int newCategoryId) throws IOException;

  // ブログ記事削除
  void deleteArticle(long userId, long articleId) throws IOException;

  // ブログ記事の取得
  Article getArticle(long userId, long articleId) throws IOException;
  
  // ブログ記事の取得(最新順)
  List<Article> getArticles(long userId, Long lastArticleId, int length) throws IOException;

  // ブログ記事の取得(カテゴリ別)
  List<Article> getArticles(long userId, int categoryId, Long lastArticleId, int length) throws IOException;
}
