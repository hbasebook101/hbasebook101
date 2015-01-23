package document;

import java.io.IOException;
import java.util.List;

public interface DocumentVersionManager {
  // ドキュメントを保存する
  void save(String documentId, String title, String text) throws IOException;

  // ドキュメントのバージョンの一覧を取得する
  List<Long> listVersions(String documentId) throws IOException;

  // ドキュメントの最新バージョンを取得する
  Document getLatest(String documentId) throws IOException;

  // ドキュメントをバージョンを指定して取得する
  Document get(String documentId, long version) throws IOException;
}
