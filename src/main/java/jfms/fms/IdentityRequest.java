package jfms.fms;

import java.io.ByteArrayInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.xml.IdentityParser;
import jfms.store.Store;

public class IdentityRequest extends DownloadRequest {

	private static final Logger LOG = Logger.getLogger(IdentityRequest.class.getName());
	private static final IdentityParser identityParser = new IdentityParser();

	private final int identityId;
	private final String ssk;
	private final SuccessAction successAction;
	private final MessageReferenceList messageList;
	private final RequestTracker requestTracker;
	private final MessageRequest messageRequest;

	public enum SuccessAction {
		NONE,
		REQUEST_TRUSTLIST,
		REQUEST_MESSAGELIST,
		REQUEST_MESSAGE
	}

	public IdentityRequest(String id, int identityId, String ssk,
			RequestTracker requestTracker, SuccessAction successAction) {

		this(id, identityId, ssk, requestTracker, successAction, null, null);
	}

	public IdentityRequest(String id, int identityId, String ssk,
			RequestTracker requestTracker, SuccessAction successAction,
			MessageRequest messageRequest) {

		this(id, identityId, ssk, requestTracker, successAction, null,
				messageRequest);
	}

	public IdentityRequest(String id, int identityId, String ssk,
			RequestTracker requestTracker, SuccessAction successAction,
			MessageReferenceList messageList,
			MessageRequest messageRequest) {

		super(id, Identity.getIdentityKey(ssk, requestTracker.getDate(),
				requestTracker.getIndex()), Constants.TTL_IDENTITY);

		// TODO validate parameters
		this.requestTracker = requestTracker;
		this.identityId = identityId;
		this.ssk = ssk;
		this.successAction = successAction;
		this.messageList = messageList;
		this.messageRequest = messageRequest;
	}

	@Override
	public void finished(byte[] data) {
		Store store = FmsManager.getInstance().getStore();
		Identity identity = identityParser.parse(new ByteArrayInputStream(data));
		if (identity == null) {
			error(Constants.CODE_PARSE_FAILED);
			return;
		}

		identity.setSsk(ssk);
		LOG.log(Level.FINE, "retrieved Identity of {0}",
				identity.getFullName());

		IdentityManager identityManager
				= FmsManager.getInstance().getIdentityManager();
		identityManager.updateIdentity(identityId, identity);

		store.updateRequestHistory(identityId, RequestType.IDENTITY,
				requestTracker.getDate(), requestTracker.getIndex());
		if (requestTracker.getFailDate() != null) {
			LOG.log(Level.FINEST, "saving fail date {0}",
					requestTracker.getFailDate());
			store.updateLastFailDate(identityId, requestTracker.getFailDate());
		}

		switch (successAction) {
		case NONE:
			break;
		case REQUEST_TRUSTLIST:
			if (!identity.getPublishTrustList()) {
				break;
			}

			RequestTracker trustListTracker = RequestTracker.create(
					RequestType.TRUST_LIST, identityId,
					requestTracker.getDate(),
					Constants.MAX_TRUSTLIST_INDEX);

			if (trustListTracker != null) {
				trustListTracker = trustListTracker
					.setFailDate(requestTracker.getFailDate());
				setChainedRequest(new TrustListRequest(getNextId(),
							identityId, ssk, trustListTracker));
			}
			break;
		case REQUEST_MESSAGELIST:
			RequestTracker messageListTracker = RequestTracker.create(
					RequestType.MESSAGE_LIST, identityId,
					requestTracker.getDate(),
					Constants.MAX_MESSAGELIST_INDEX);
			if (messageListTracker != null) {
				messageListTracker = messageListTracker
					.setFailDate(requestTracker.getFailDate());
				setChainedRequest(new MessageListRequest(getNextId(),
						identityId, ssk,
						messageList, messageListTracker));
			}
			break;
		case REQUEST_MESSAGE:
			messageRequest.setId(getNextId());
			setChainedRequest(messageRequest);
			break;
		}
	}

	@Override
	public boolean redirect(String redirectURI) {
		LOG.log(Level.WARNING, "got unexpected redirect");

		return false;
	}

	@Override
	public void error(int code) {
		if (code != Constants.CODE_RECENTLY_TRIED) {
			LOG.log(Level.FINEST, "failed to retrieve {0}", getKey());
		}

		if (successAction == SuccessAction.REQUEST_MESSAGE) {
			messageRequest.setId(getNextId());
			setChainedRequest(messageRequest);
			return;
		}

		if (code == Constants.CODE_RECENTLY_TRIED) {
			return;
		}

		if (requestTracker.getFastMessageCheckEnabled()) {
			LOG.log(Level.FINEST, "fast message check mode: skipping further requests");
			return;
		}

		requestTracker.setFail();

		RequestTracker newTracker = requestTracker.minusDays(1);
		if (newTracker != null) {
			LOG.log(Level.FINEST, "retrying with date {0}", newTracker.getDate());
			setChainedRequest(new IdentityRequest(getNextId(),
					identityId, ssk, newTracker,
					successAction, messageList, null));
		} else {
			// mark day as permanently failed
			if (requestTracker.getFailDate() != null) {
				LOG.log(Level.FINEST, "saving fail date {0}", requestTracker.getFailDate());
				Store store = FmsManager.getInstance().getStore();
				store.updateLastFailDate(identityId, requestTracker.getFailDate());
			}
		}
	}

	@Override
	public int isSuccessful() {
		return requestTracker.getSuccessCount();
	}
}
