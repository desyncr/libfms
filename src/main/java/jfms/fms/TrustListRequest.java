package jfms.fms;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.xml.TrustListParser;
import jfms.store.Store;

public class TrustListRequest extends DownloadRequest {
	private static final Logger LOG = Logger.getLogger(TrustListRequest.class.getName());
	private static final TrustListParser trustListParser = new TrustListParser();

	private final int identityId;
	private final String ssk;
	private final RequestTracker requestTracker;

	public TrustListRequest(String id, int identityId, String ssk,
			RequestTracker requestTracker) {

		super(id, Identity.getTrustListKey(ssk,
				requestTracker.getDate(), requestTracker.getIndex()),
				Constants.TTL_TRUSTLIST);
		this.identityId = identityId;
		this.ssk = ssk;
		this.requestTracker = requestTracker;
	}

	@Override
	public void finished(byte[] data) {
		requestTracker.setSuccess();

		Store store = FmsManager.getInstance().getStore();
		IdentityManager identityManager = FmsManager.getInstance().getIdentityManager();

		List<Trust> trusts = trustListParser.parse(new ByteArrayInputStream(data));

		Map<Integer, String> newIdentities =
			store.saveTrustList(identityId, trusts);
		if (newIdentities != null) {
			for (Map.Entry<Integer,String> e : newIdentities.entrySet()) {
				identityManager.addIdentityFromTrustList(
						e.getKey(), e.getValue());
			}
		}

		store.updateRequestHistory(identityId, RequestType.TRUST_LIST,
				requestTracker.getDate(), requestTracker.getIndex());

		final RequestTracker nextTracker = requestTracker.incrementIndex();
		if (nextTracker != null) {
			setChainedRequest(new TrustListRequest(getNextId(), identityId, ssk,
					nextTracker));
		}
	}

	@Override
	public boolean redirect(String redirectURI) {
		LOG.log(Level.FINEST, "got unexpected redirect");

		return false;
	}

	@Override
	public int isSuccessful() {
		return requestTracker.getSuccessCount();
	}
}
