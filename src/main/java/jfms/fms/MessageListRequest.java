package jfms.fms;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jfms.config.Constants;
import jfms.fms.xml.MessageListParser;
import jfms.store.Store;

public class MessageListRequest extends DownloadRequest {
	private static final Logger LOG = Logger.getLogger(MessageListRequest.class.getName());
	private static final Pattern MSGLIST_PATTERN = Pattern.compile("USK@.*MessageList/(\\d+)/MessageList.xml");

	private final int identityId;
	private final String ssk;
	private final MessageReferenceList messageList;
	private RequestTracker requestTracker;

	public MessageListRequest(String id, int identityId, String ssk,
		MessageReferenceList messageList,
		RequestTracker requestTracker) {

		super(id, Identity.getMessageListKey(ssk,
				requestTracker.getDate(), requestTracker.getIndex(), true),
				Constants.TTL_MESSAGELIST);
		this.identityId = identityId;
		this.ssk = ssk;
		this.messageList = messageList;
		this.requestTracker = requestTracker;
	}

	@Override
	public List<String> getAdditionalFields() {
		return Arrays.asList("IgnoreUSKDatehints=true");
	}

	@Override
	public void finished(byte[] data) {
		requestTracker.setSuccess();

		MessageListParser messageListParser = new MessageListParser();
		List<MessageReference> xmlMessageList = messageListParser.parse(new ByteArrayInputStream(data));

		IdentityManager identityManager = FmsManager.getInstance().getIdentityManager();
		Identity id = identityManager.getIdentity(identityId);

		LOG.log(Level.FINEST, "Retrieved MessageList of {0} containing "
				+ "{1} messages", new Object[]{
				id.getFullName(), xmlMessageList.size()});

		TrustManager trustManager = FmsManager.getInstance().getTrustManager();
		boolean includeNullPeerTrust = true;

		for (MessageReference m : xmlMessageList) {
			final String msgSsk = m.getSsk();
			if (msgSsk == null) {
				m.setIdentityId(identityId);
				m.setSsk(ssk);
			} else {
				Integer msgIdentityId = identityManager.getIdentityId(msgSsk);
				if (msgIdentityId == null) {
					LOG.log(Level.FINEST, "Skipping unknown ID {0} "
							+ "in MessageList", msgSsk);
					continue;
				}
				if (!trustManager.isMessageTrusted(msgIdentityId, includeNullPeerTrust)) {
					LOG.log(Level.FINEST, "Skipping untrusted ID {0} "
							+ "in MessageList", msgSsk);
					continue;
				}
				m.setIdentityId(msgIdentityId);
			}

			messageList.addMessageToDownload(m);
		}

		messageList.cleanup();
		if (messageList.isEmpty()) {
			if (requestTracker.getFastMessageCheckEnabled()) {
				LOG.log(Level.FINEST, "fast message check mode: skipping further requests");
				return;
			}
			final Store store = FmsManager.getInstance().getStore();
			store.updateRequestHistory(identityId,
					RequestType.MESSAGE_LIST,
					requestTracker.getDate(),
					requestTracker.getIndex());

			RequestTracker nextTracker =
				requestTracker.incrementIndex();
			if (nextTracker != null)  {
				setChainedRequest(new MessageListRequest(getNextId(),
							identityId,
							ssk,
							messageList,
							nextTracker));
			}
		} else {
			MessageReference msg = messageList.remove();
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
				// propagate success from MessageListRequests to chained
				// MessageRequests (for statistics only)
				msgTracker.setSuccess();
				setChainedRequest(new IdentityRequest(getNextId(),
					msg.getIdentityId(), msg.getSsk(), msgTracker,
					IdentityRequest.SuccessAction.REQUEST_MESSAGE,
					msgRequest));
			}
		}
	}

	@Override
	public boolean redirect(String redirectURI) {
		Matcher m = MSGLIST_PATTERN.matcher(redirectURI);
		if (m.matches() && m.groupCount() > 0) {
			int index = Integer.parseInt(m.group(1));
			LOG.log(Level.FINEST, "got redirect, updating index to {0}", index);
			requestTracker = requestTracker.setIndex(index);
		} else {
			LOG.log(Level.WARNING, "failed to parse redirectURI");
			return false;
		}

		setKey(redirectURI);

		return true;
	}

	@Override
	public int isSuccessful() {
		return requestTracker.getSuccessCount();
	}
}
