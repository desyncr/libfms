package jfms.fms;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.store.InsertStatus;
import jfms.store.Store;

public class MessageManager {
	private static final Logger LOG = Logger.getLogger(MessageManager.class.getName());
	private MessageListener listener;

	public static class MessageAdder implements Runnable {
		private final MessageListener messageListener;
		private final Message message;

		public MessageAdder(MessageListener listener, Message message) {
			this.messageListener = listener;
			this.message = message;
		}

		@Override
		public void run() {
			messageListener.newMessage(message);
		}
	}


	public void addMessage(Message message) {
		final Store store = FmsManager.getInstance().getStore();
		final BoardManager boardManager = FmsManager.getInstance().getBoardManager();

		Map<Integer,String> newBoards = new HashMap<>();
		int messageId = store.saveMessage(message, newBoards);
		if (messageId == -1) {
			return;
		}

		for (Map.Entry<Integer,String> e : newBoards.entrySet()) {
			boardManager.addBoard(e.getKey(), e.getValue());
		}

		message.setMessageId(messageId);
		if (listener != null) {
			listener.newMessage(message);
		}
	}

	public void addLocalMessage(MessageReference msgRef, InsertStatus status,
			InsertStatus previousStatus) {
		if (listener != null) {
			listener.newLocalMessage(msgRef, status, previousStatus);
		}
	}

	public void setListener(MessageListener listener) {
		this.listener = listener;
	}

	public void deleteQueuedMessage(int localIdentityId,
			LocalDate date, int index) {

		InsertThread insertThread = FmsManager.getInstance().getInsertThread();
		LOG.log(Level.FINE, "deleting queued message from outbox");

		if (insertThread != null) {
			insertThread.cancelMessage(localIdentityId, date, index);
		}

		FmsManager.getInstance().getStore().deleteLocalMessage(
				localIdentityId, date, index);
	}
}
