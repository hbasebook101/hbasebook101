package document;

public class Document {
  // ドキュメントID
  private String documentId;

  // バージョン番号
  private long version;

  // タイトル
  private String title;

  // テキスト
  private String text;

  public String getDocumentId() {
    return documentId;
  }

  public void setDocumentId(String documentId) {
    this.documentId = documentId;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "Document [documentId=" + documentId + ", title=" + title + ", text=" + text + ", version=" + version + "]";
  }
}
