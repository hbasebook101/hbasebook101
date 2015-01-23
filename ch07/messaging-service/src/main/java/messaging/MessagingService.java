package messaging;

import java.io.IOException;
import java.util.List;

public interface MessagingService {
  // メッセージを送信する
  void sendMessage(long roomId, long userId, String body) throws IOException;

  // ルームに初めて入った時に、他のユーザによって既に交換されているメッセージを取得する
  List<Message> getInitialMessages(long roomId, List<Long> blockUsers) throws IOException;

  // 過去のメッセージを取得する
  List<Message> getOldMessages(long roomId, Message oldestReceivedMessage, List<Long> blockUsers) throws IOException;

  // 最新メッセージを取得する
  List<Message> getNewMessages(long roomId, Message latestReceivedMessage, List<Long> blockUsers) throws IOException;
}
