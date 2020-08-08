package jfms.fms;

import jfms.store.InsertStatus;

public interface MessageListener {
	void newMessage(Message message);
	void newLocalMessage(MessageReference msgRef, InsertStatus status,
			InsertStatus previousStatus);
}
