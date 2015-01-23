package document;

import java.io.IOException;
import java.util.List;

public class Main {
  public static void main(String[] args) throws IOException, InterruptedException {
    DocumentVersionManager versionManager = new DocumentVersionManagerImpl();
    versionManager.save("doc1", "test1", "aaabbbccdd");
    versionManager.save("doc1", "test1", "aaabbbccddeeff");
    List<Long> listVersions = versionManager.listVersions("doc1");
    System.out.println(listVersions);
    System.out.println(versionManager.get("doc1", listVersions.get(1)));
    System.out.println("-----");
  }
}
