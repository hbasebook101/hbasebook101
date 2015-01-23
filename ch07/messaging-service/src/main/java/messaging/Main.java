package messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//動作確認用
public class Main {
	public static void main(String[] args) throws IOException, InterruptedException {
		MessagingService messagingService = new MessagingServiceImpl();
		messagingService.sendMessage(1, 2, "hoge hoge.");
		messagingService.sendMessage(1, 1, "foo bar.");
		messagingService.sendMessage(1, 2, "Hello.");
		messagingService.sendMessage(1, 1, "Hi.");
		for (int i = 0; i < 50; i++) {
		  messagingService.sendMessage(2, (i%2), "message_" + i);
		}
		
		List<Long> filterUsers = new ArrayList<>();
		filterUsers.add(0L);
		
		List<Message> reciveMessages = messagingService.getInitialMessages(2, filterUsers);
		for (Message message : reciveMessages) {
			System.out.println(message.getUserId() + " : " + message.getBody());
		}
		Message lastMessage = reciveMessages.get(reciveMessages.size() - 1);
		
    System.out.println("-----");
    
    for (int i = 0; i < 50; i++) {
      messagingService.sendMessage(2, (i%2), "message_" + (i + 50));
    }

    reciveMessages = messagingService.getNewMessages(2, reciveMessages.get(0), filterUsers);
    for (Message message : reciveMessages) {
      System.out.println(message.getUserId() + " : " + message.getBody());
    }
    
		System.out.println("-----");
		
		reciveMessages = messagingService.getOldMessages(2, lastMessage, filterUsers);
    for (Message message : reciveMessages) {
      System.out.println(message.getUserId() + " : " + message.getBody());
    }
	}
}
