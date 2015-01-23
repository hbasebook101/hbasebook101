package blog;

import java.io.IOException;
import java.util.List;

// 動作確認用
public class Main {
  public static void main(String[] args) throws IOException {
    BlogService blogService = new BlogServiceImpl();
    blogService.postArticle(1, "title1-1", "aa bb cc", 1);
    blogService.postArticle(1, "title1-2", "dd ee ff gg", 2);
    blogService.postArticle(1, "title1-3", "aa bb", 1);
    blogService.postArticle(2, "title2-1", "aa bb cc", 1);
    blogService.postArticle(2, "title2-2", "dd", 2);
    blogService.postArticle(1, "title1-4", "HH II", 1);
    blogService.postArticle(1, "title1-5", "JJ", 2);
    blogService.postArticle(1, "title1-6", "LL", 2);
    blogService.postArticle(1, "title1-7", "LL MM NN bb", 2);

    
    List<Article> articles = blogService.getArticles(1, null, 3);
    for (Article article : articles) {
      System.out.println(article.getTitle());
    }
    
    System.out.println("-----");
    
    articles = blogService.getArticles(1, articles.get(articles.size() - 1).getArticleId(), 3);
    for (Article article : articles) {
      System.out.println(article.getTitle());
    }
    
    Article article2 = articles.get(articles.size() - 1);
    Article article3 = blogService.getArticle(1, article2.getArticleId());
    System.out.println(article3.getTitle() + ":" + article3.getContent());
    
    System.out.println("-----");

    articles = blogService.getArticles(1, 2, null, 2);
    for (Article article : articles) {
      System.out.println(article.getTitle());
    }
    
    System.out.println("-----");

    articles = blogService.getArticles(1, 2, articles.get(articles.size() - 1).getArticleId(), 2);
    for (Article article : articles) {
      System.out.println(article.getTitle());
    }
    
    Article updateTarget = articles.get(articles.size() - 1);
    blogService.updateArticle(1, updateTarget.getArticleId(), updateTarget.getTitle() + "_update", updateTarget.getContent(), updateTarget.getCategoryId() + 1);
    
    System.out.println("-----");

    Article article = blogService.getArticle(1, updateTarget.getArticleId());
    System.out.println(article.getTitle());
    
    System.out.println("-----");
    blogService.deleteArticle(1, article.getArticleId());
    blogService.getArticle(1, updateTarget.getArticleId());
  }
}
