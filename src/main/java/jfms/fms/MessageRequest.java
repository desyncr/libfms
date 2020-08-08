package jfms.fms;

import java.io.ByteArrayInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.xml.MessageParser;
import jfms.store.Store;
import jfms.util.UUID;

public class MessageRequest extends DownloadRequest {
	private static final Logger LOG = Logger.getLogger(MessageRequest.class.getName());
	private static final MessageParser messageParser = new MessageParser();

	private final int identityId;
	private final String ssk;
	private final MessageReference messageRef;
	private final MessageReferenceList messageList;
	private final RequestTracker requestTracker;

	public MessageRequest(String id, int identityId, String ssk,
			MessageReference msg,
			MessageReferenceList messageList,
			RequestTracker requestTracker) {

		super(id, Identity.getMessageKey(msg.getSsk(),
				msg.getDate(), msg.getIndex()),
				Constants.TTL_MESSAGE);
		this.identityId = identityId;
		this.ssk = ssk;
		this.messageRef = msg;
		this.messageList = messageList;
		this.requestTracker = requestTracker;
	}

	@Override
	public void finished(byte[] data) {
		parseAndStore(data);
		chainNextRequest();
	}

	@Override
	public void error(int code) {
		chainNextRequest();
	}

	@Override
	public int isSuccessful() {
		return requestTracker.getSuccessCount();
	}

	private void parseAndStore(byte[] data) {
		Message message = messageParser.parse(new ByteArrayInputStream(data));
		if (message == null) {
			LOG.log(Level.INFO, "failed to parse message from {0}",
					getKey());
			return;
		}

		LOG.log(Level.FINE, "retrieved Message in Board {0} "
				+ "with Subject {1}", new Object[]{
				message.getReplyBoard(), message.getSubject()});
		message.setIdentityId(messageRef.getIdentityId());
		message.setInsertDate(messageRef.getDate());
		message.setInsertIndex(messageRef.getIndex());

		InReplyTo inReplyTo = message.getInReplyTo();
		if (inReplyTo != null) {
			message.setParentId(inReplyTo.getParentMessageId());
		}

		final String uuid = message.getMessageUuid();
		if (!UUID.check(messageRef.getSsk(), uuid)) {
			LOG.log(Level.INFO, "wrong message UUID {0} ", uuid);
			return;
		}

		FmsManager.getInstance().getMessageManager().addMessage(message);
	}

	private void chainNextRequest() {
		MessageReference msg = messageList.remove();

		if (msg != null) {
			MessageRequest msgRequest = new MessageRequest(null,
					identityId, ssk,
					msg,
					messageList, requestTracker);

			// check if we should update identity first
			RequestTracker msgTracker = RequestTracker.createSingleRequest(
					RequestType.IDENTITY, msg.getIdentityId(), msg.getDate());
			if (msgTracker == null) {
				msgRequest.setId(getNextId());
				setChainedRequest(msgRequest);
			} else {
				setChainedRequest(new IdentityRequest(getNextId(),
					msg.getIdentityId(), msg.getSsk(), msgTracker,
					IdentityRequest.SuccessAction.REQUEST_MESSAGE,
					msgRequest));
			}
		} else {
			if (identityId < 0) {
				// we are running message requests for a different ID
				// -> no jfms.store update, no further msg list requests
				return;
			}

			if (requestTracker.getFastMessageCheckEnabled()) {
				LOG.log(Level.FINEST, "fast message check mode: skipping further requests");
				return;
			}

			final Store store = FmsManager.getInstance().getStore();
			store.updateRequestHistory(identityId,
					RequestType.MESSAGE_LIST,
					requestTracker.getDate(),
					requestTracker.getIndex());

			final RequestTracker nextTracker =
				requestTracker.incrementIndex();
			if (nextTracker != null) {
				setChainedRequest(new MessageListRequest(getNextId(),
							identityId,
							ssk,
							messageList,
							nextTracker));
			}
		}
	}
}
