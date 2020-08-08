package jfms.fms;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import jfms.config.Constants;
import jfms.fcp.FcpClient;
import jfms.fcp.FcpException;
import jfms.fcp.FcpListener;
import jfms.store.Store;
import jfms.util.RequestID;
import jfms.util.RequestLimiter;

public class DownloadThread implements FcpListener, Runnable {
	private static final Logger LOG = Logger.getLogger(DownloadThread.class.getName());
	private static int iteration = 0;

	private final Map<String, DownloadRequest> fcpRequests = new ConcurrentHashMap<>();
	private final FcpClient fcpClient;
	private ProgressListener progressListener;

	private int successfulCount;
	private int failedCount;
	private int totalCount;
	private String countLabel;
	private final RequestID requestID = new RequestID("request-");
	private RequestLimiter requestLimiter;
	private final MessageReferenceList globalMessageList = new MessageReferenceList();

	public enum Mode {
		ACTIVE,
		INACTIVE
	}

	public static void resetIteration() {
		iteration = 0;
	}

	public DownloadThread(FcpClient fcpClient) {
		this.fcpClient = fcpClient;
	}

	public void setProgressListener(ProgressListener progressListener) {
		this.progressListener = progressListener;
	}

	private void updateProgress(long workDone, long max) {
		if (progressListener != null) {
			progressListener.updateProgress(workDone, max);
		}
	}

	private void updateTitle(String title) {
		if (progressListener != null) {
			progressListener.updateTitle(title);
		}
	}

	private void updateMessage(String message) {
		if (progressListener != null) {
			progressListener.updateMessage(message);
		}
	}

	@Override
	public void run() {
		++iteration;
		Thread.currentThread().setName("DownloadThread");
		LOG.log(Level.FINE, "Started FMS download thread with ID {0}",
				Thread.currentThread().getId());
		updateTitle("Idle");
		updateMessage("No requests pending");
		updateProgress(0, 1);

		requestLimiter = new RequestLimiter();

		try {
			fcpClient.start();
			fcpClient.cleanup();

			final LocalDate today = LocalDate.now(ZoneOffset.UTC);

			download(today, Mode.ACTIVE, true);
			download(today, Mode.INACTIVE, true);
			downloadAvatars(today);
		} catch (InterruptedException e) {
			LOG.log(Level.FINE, "FMS download thread interrupted");
		} catch (Exception e) {
			LOG.log(Level.WARNING, "exception in FMS download thread", e);
		}

		updateTitle("Idle");
		updateMessage("No requests pending");
		updateProgress(0, 1);
		LOG.log(Level.FINE, "FMS download thread stopped");
	}

	public void updateDownloadProgress() {
		final int messageCount = successfulCount + failedCount;

		StringBuilder msg = new StringBuilder("Requested ");
		msg.append(messageCount);
		msg.append('/');
		msg.append(totalCount);
		msg.append(' ');
		msg.append(countLabel);
		msg.append(" (");
		msg.append(successfulCount);
		msg.append(" new)");
		updateMessage(msg.toString());

		updateProgress(messageCount, totalCount);
	}

	@Override
	public void error(String fcpIdentifier, int code) {
		LOG.log(Level.FINE, "Request [{0}] failed", fcpIdentifier);

		boolean lastInChain = true;
		int success = 0;
		try {
			DownloadRequest fcpRequest = fcpRequests.remove(fcpIdentifier);
			if (fcpRequest != null) {
				fcpRequest.error(code);

				DownloadRequest chainedRequest = fcpRequest.getChainedRequest();
				if (chainedRequest != null) {
					lastInChain = false;
					LOG.log(Level.FINEST, "Got chained request!");
					queueFcpRequest(chainedRequest);
				}

				success = fcpRequest.isSuccessful();
			} else {
				LOG.log(Level.WARNING, "got FCP response for unknown ID: {0}", fcpIdentifier);
			}
		} catch (Exception e) {
			LOG.log(Level.WARNING, "Failed to handle FCP error response", e);
		}

		if (lastInChain && success >= 0) {
			if (success > 0) {
				successfulCount++;
			} else {
				failedCount++;
			}
			updateDownloadProgress();

			addMessageRequests();
		}

		requestLimiter.requestDone();
	}

	@Override
	public void finished(String fcpIdentifier, byte[] data) {
		LOG.log(Level.FINEST,
				"request finished: ID {0} in Thread {1}", new Object[]{
				fcpIdentifier, Thread.currentThread().getId()});

		boolean lastInChain = true;
		int success = 0;
		try {
			DownloadRequest fcpRequest = fcpRequests.remove(fcpIdentifier);
			if (fcpRequest != null) {
				fcpRequest.finished(data);

				DownloadRequest chainedRequest = fcpRequest.getChainedRequest();
				if (chainedRequest != null) {
					lastInChain = false;
					LOG.log(Level.FINEST, "Got chained request!");
					queueFcpRequest(chainedRequest);
				}

				success = fcpRequest.isSuccessful();
			} else {
				LOG.log(Level.WARNING, "got FCP response for unknown ID: {0}",
						fcpIdentifier);
			}
		} catch (FcpException e) {
			LOG.log(Level.WARNING, "Failed to handle FCP finished response", e);
		}

		if (lastInChain && success >= 0) {
			if (success > 0) {
				successfulCount++;
			} else {
				failedCount++;
			}
			updateDownloadProgress();

			addMessageRequests();
		}

		requestLimiter.requestDone();
	}

	@Override
	public void redirect(String fcpIdentifier, String redirectURI) {
		LOG.log(Level.FINEST,
				"request redirected: ID {0} in Thread {1}", new Object[]{
				fcpIdentifier, Thread.currentThread().getId()});
		try {
			DownloadRequest fcpRequest = fcpRequests.get(fcpIdentifier);
			if (fcpRequest != null) {
				if (fcpRequest.redirect(redirectURI)) {
					queueFcpRequest(fcpRequest);
				}
			} else {
				LOG.log(Level.WARNING, "got FCP response for unknown ID: {0}",
						fcpIdentifier);
			}
		} catch (Exception e) {
			LOG.log(Level.WARNING, "Failed to handle FCP response", e);
		}

		requestLimiter.requestDone();
	}

	@Override
	public void putSuccessful(String fcpIdentifier, String key) {
		LOG.log(Level.WARNING, "unexpected putSuccessful");
	}

	@Override
	public void keyPairGenerated(String fcpIdentifier, String publicKey,
			String privateKey) {

		LOG.log(Level.WARNING, "unexpected keyPairGenerated");
	}

	private void queueFcpRequest(DownloadRequest fcpRequest)
			throws FcpException {

		DownloadRequest request = fcpRequest;
		while (request != null) {
			if (queueFcpRequestIfNotInCache(fcpRequest)) {
				break;
			}
			request.error(Constants.CODE_RECENTLY_TRIED);
			request = request.getChainedRequest();
		}
	}

	private boolean queueFcpRequestIfNotInCache(DownloadRequest fcpRequest)
		throws FcpException {

		final String key = fcpRequest.getKey();
		final String id = fcpRequest.getId();

		if (fcpClient.requestKey(id, key, this, fcpRequest.getTTL(),
					fcpRequest.getAdditionalFields())) {
			fcpRequests.put(id, fcpRequest);
			requestLimiter.addRequest();
			return true;
		} else {
			return false;
		}
	}

	private void download(LocalDate date, Mode mode, Boolean isFastMessageCheckEnabled)
		throws InterruptedException, FcpException {

		LOG.log(Level.FINEST, "Starting downloading for {0} identities",
				mode.toString().toLowerCase(Locale.ENGLISH));

		TrustManager trustManager = FmsManager.getInstance().getTrustManager();
		if (trustManager.getTrustListTrustedIds().isEmpty()) {
			LOG.log(Level.WARNING, "No trusted IDs found, skipping download");
			return;
		}

		if (iteration == 1 && mode == Mode.ACTIVE &&
				isFastMessageCheckEnabled) {

			LOG.log(Level.FINE, "Performing fast message check");
			downloadMessageLists(date, mode, true);
		}

		boolean retry = true;
		while (retry) {
			retry = downloadTrustLists(date, mode);
		}

		downloadMessageLists(date, mode);
		downloadIdentityIntroductions(date);

		LOG.log(Level.FINEST, "Finished downloading for {0} identities",
				mode.toString().toLowerCase(Locale.ENGLISH));
	}

	private void downloadAvatars(LocalDate date)
		throws FcpException,InterruptedException {

		LOG.log(Level.FINEST, "Start downloading avatars");

		List<DownloadRequest> requests = new ArrayList<>();

		final Store store = FmsManager.getInstance().getStore();
		store.populateAvatarTable();

		List<Avatar> avatars = store.getMissingAvatars();

		for (Avatar a : avatars) {
			final LocalDate lastFailDate = a.getLastFailDate();
			int tries = a.getTries();
			if (tries > 8) {
				// too many retries, give up
				continue;
			}
			if (lastFailDate != null) {
				LocalDate nextTry =
					lastFailDate.plusDays((2 << a.getTries()) - 1);
				if (nextTry.isAfter(date)) {
					continue;
				}
			}

			DownloadRequest request = new AvatarRequest(null,
					a.getIdentityId(), a.getKey());
			requests.add(request);
		}

		downloadRequests("avatars", requests);

		LOG.log(Level.FINEST, "Waiting for avatar downloads to finish");
		requestLimiter.waitUntilReady(1);

		LOG.log(Level.FINEST, "Finished downloading avatars");
	}

	private void downloadRequests(String label, List<DownloadRequest> requests)
		throws FcpException, InterruptedException {

		countLabel = label;
		successfulCount = 0;
		failedCount = 0;
		totalCount = requests.size();

		StringBuilder str = new StringBuilder();
		str.append("Requesting ");
		str.append(label);
		updateTitle(str.toString());
		updateDownloadProgress();

		final int maxFcpRequests = Integer.parseInt(Constants.DEFAULT_MAX_FCP_REQUESTS);
		for (DownloadRequest request : requests) {
			requestLimiter.waitUntilReady(maxFcpRequests);
			request.setId(requestID.getNext());
			queueFcpRequest(request);
		}
	}

	private DownloadRequest createTrustListRequest(int identityId, LocalDate date) {
		final IdentityManager identityManager =
				FmsManager.getInstance().getIdentityManager();
		final Identity identity = identityManager.getIdentity(identityId);
		final String ssk = identity.getSsk();

		RequestTracker identityTracker = RequestTracker.create(
				RequestType.IDENTITY, identityId, date,
				Constants.MAX_IDENTITY_INDEX);

		DownloadRequest request;
		if (identityTracker != null) {
			// identity is outdated; create identity request with chained
			// trust list request on success

			request = new IdentityRequest(null, identityId, ssk,
					identityTracker,
					IdentityRequest.SuccessAction.REQUEST_TRUSTLIST);
		} else {
			// identity is up-to-date; request trust list directly
			if (!identity.getPublishTrustList()) {
				// identity does not publish a trust list
				return null;
			}

			RequestTracker trustListTracker = RequestTracker.create(
					RequestType.TRUST_LIST, identityId, date,
					Constants.MAX_TRUSTLIST_INDEX);
			if (trustListTracker == null) {
				return null;
			}
			request = new TrustListRequest(null, identityId, ssk,
				trustListTracker);
		}

		if (fcpClient.isRecentlyFailed(request.getKey())) {
			request = null;
		}

		return request;
	}

	private boolean downloadTrustLists(LocalDate date, Mode mode)
		throws InterruptedException, FcpException {

		TrustManager trustManager = FmsManager.getInstance().getTrustManager();
		Set<Integer> trustlistTrustedIds = trustManager.getTrustListTrustedIds();

		List<DownloadRequest> requests = new ArrayList<>();

		int processed = 0;
		for (int identityId : getEligibleIdentities(date, mode)) {
			if (!trustlistTrustedIds.contains(identityId)) {
				continue;
			}

			DownloadRequest request = createTrustListRequest(identityId, date);
			if (request != null) {
				requests.add(request);
			}

			processed++;
		}

		LOG.log(Level.FINEST,
				"Checked {0,number,0}/{1,number,0} trust lists", new Object[]{
				processed, trustlistTrustedIds.size()});
		LOG.log(Level.FINEST, "Requesting {0,number,0} trust lists",
				requests.size());

		if (requests.isEmpty()) {
			return false;
		}

		final String label;
		if (mode == Mode.ACTIVE) {
			label = "trust lists";
		} else {
			label = "inactive trust lists";
		}
		downloadRequests(label, requests);

		LOG.log(Level.FINEST, "Waiting for trust lists to finish");
		requestLimiter.waitUntilReady(1);
		LOG.log(Level.FINE, "trust list download finished");

		if (successfulCount > 0) {
			trustManager.initialize();
			return true;
		} else {
			return false;
		}
	}

	private DownloadRequest createMessageListRequest(int identityId,
			LocalDate date, MessageReferenceList globalMessageList,
			boolean fastMessageCheck) {

		final IdentityManager identityManager =
				FmsManager.getInstance().getIdentityManager();
		final Identity identity = identityManager.getIdentity(identityId);
		final String ssk = identity.getSsk();

		RequestTracker identityTracker = RequestTracker.create(
				RequestType.IDENTITY, identityId, date,
				Constants.MAX_IDENTITY_INDEX);

		DownloadRequest request;
		if (identityTracker != null) {
			// identity is outdated; create identity request with chained
			// trust list request on success

			if (fastMessageCheck) {
				identityTracker.setFastMessageCheckEnabled(true);
			}

			request = new IdentityRequest(null, identityId, ssk,
					identityTracker,
					IdentityRequest.SuccessAction.REQUEST_MESSAGELIST,
					globalMessageList,
					null);
		} else {
			// identity is up-to-date; request message list directly
			RequestTracker messageListTracker = RequestTracker.create(
					RequestType.MESSAGE_LIST, identityId, date,
					Constants.MAX_MESSAGELIST_INDEX);
			if (messageListTracker == null) {
				return null;
			}
			if (fastMessageCheck) {
				messageListTracker.setFastMessageCheckEnabled(true);
			}
			request = new MessageListRequest(null, identityId, ssk,
					globalMessageList, messageListTracker);
		}

		if (fastMessageCheck) {
			// don't create negative cache entries
			// otherwise we will skip days backwards in regular mode
			request.setTTL(0);
		}

		if (fcpClient.isRecentlyFailed(request.getKey())) {
			request = null;
		}

		return request;
	}

	private void downloadMessageLists(LocalDate date, Mode mode)
		throws InterruptedException, FcpException {

		downloadMessageLists(date, mode, false);
	}

	private void downloadMessageLists(LocalDate date, Mode mode,
			boolean fastMessageCheck)

		throws InterruptedException, FcpException {

		TrustManager trustManager = FmsManager.getInstance().getTrustManager();
		Set<Integer> trustedIds = trustManager.getMessageTrustedIds();

		List<DownloadRequest> requests = new ArrayList<>();
		globalMessageList.clear();

		int processed = 0;
		for (int identityId : getEligibleIdentities(date, mode)) {
			if (fastMessageCheck && processed >= Constants.MAX_FAST_MESSAGE_CHECK_COUNT) {
				break;
			}

			if (!trustedIds.contains(identityId)) {
				continue;
			}

			DownloadRequest request = createMessageListRequest(identityId,
					date, globalMessageList, fastMessageCheck);
			if (request != null) {
				requests.add(request);
				if (mode == Mode.INACTIVE && requests.size() >=
						Constants.MAX_INACTIVE_IDENTITY_REQUESTS) {
					break;
				}
			}

			processed++;
		}

		LOG.log(Level.FINEST,
				"Checked {0,number,0}/{1,number,0} message lists",
				new Object[]{processed, trustedIds.size()});
		LOG.log(Level.FINEST, "Requesting {0,number,0} message lists",
				requests.size());

		if (requests.isEmpty()) {
			return;
		}

		String label;
		if (mode == Mode.ACTIVE) {
			label = "message lists";
		} else {
			label = "inactive message lists";
		}
		if (fastMessageCheck) {
			label += " (fast check)";
		}
		downloadRequests(label, requests);

		LOG.log(Level.FINEST, "Waiting for message lists to finish");
		requestLimiter.waitUntilReady(1);
		LOG.log(Level.FINE, "message list download finished");
	}

	private List<Integer> getEligibleIdentities(LocalDate date, Mode mode) {
		final int inactivityTimeout = Integer.parseInt(Constants.DEFAULT_INACTIVITY_TIMEOUT);
		LocalDate singleUseAddedSince =
			date.minusDays(Constants.MAX_SINGLE_USE_AGE);

		final Store store = FmsManager.getInstance().getStore();
		if (mode == Mode.ACTIVE) {
			return store.getActiveIdentities(
					date.minusDays(inactivityTimeout),
					singleUseAddedSince);
		} else {
			final int inactivityRetryInterval = Integer.parseInt(Constants.DEFAULT_INACTIVITY_RETRY_INTERVAL);
			return store.getInactiveIdentities(
					date.minusDays(inactivityTimeout),
					date.minusDays(inactivityRetryInterval),
					singleUseAddedSince);
		}
	}

	private void downloadIdentityIntroductions(LocalDate date)
			throws FcpException, InterruptedException {

		final Store store = FmsManager.getInstance().getStore();
		List<Integer> localIdentityIds = store.retrieveLocalIdentities()
			.entrySet()
			.stream()
			.map(Map.Entry::getKey)
			.collect(Collectors.toList());

		for (int localIdentityId : localIdentityIds) {
			downloadIdentityIntroduction(localIdentityId, date);
		}
	}

	private void downloadIdentityIntroduction(int localIdentityId,
			LocalDate date)
		throws InterruptedException, FcpException {

		List<DownloadRequest> requests = new ArrayList<>();
		Store store = FmsManager.getInstance().getStore();

		LocalDate fromDate = date.minusDays(Constants.MAX_PUZZLE_AGE);
		Map<DateIndex, IntroductionPuzzle> puzzles =
			store.getUnsolvedPuzzles(localIdentityId, fromDate);

		for (Map.Entry<DateIndex, IntroductionPuzzle> e : puzzles.entrySet()) {
			DateIndex dateIndex = e.getKey();
			IntroductionPuzzle puzzle = e.getValue();

			DownloadRequest request = new IdentityIntroductionRequest(null,
					localIdentityId, dateIndex.getDate(), dateIndex.getIndex(),
					puzzle.getUuid(), puzzle.getSolution());
			requests.add(request);
		}

		downloadRequests("identity introductions", requests);

		LOG.log(Level.FINEST, "Waiting for identity introductions to finish");
		requestLimiter.waitUntilReady(1);
		LOG.log(Level.FINEST, "identity introductions finished");
	}

	private void addMessageRequests() {
		if (globalMessageList.isEmpty()) {
			return;
		}

		LOG.log(Level.FINEST, "Adding message requests ({0} remaining)",
				globalMessageList.size());
		RequestTracker requestTracker = RequestTracker.create(
				Constants.FALLBACK_DATE,
				Constants.MAX_MESSAGELIST_INDEX);
		requestTracker.setSuccessCount(-1);

		MessageReference msg = globalMessageList.remove();
		MessageRequest msgRequest = new MessageRequest(null, -1, null,
				msg, globalMessageList, requestTracker);

		// check if we should update identity first
		RequestTracker msgTracker = RequestTracker.createSingleRequest(
				RequestType.IDENTITY, msg.getIdentityId(), msg.getDate());
		final DownloadRequest request;
		if (msgTracker == null) {
			msgRequest.setId(requestID.getNext());
			request = msgRequest;
		} else {
			request = new IdentityRequest(requestID.getNext(),
					msg.getIdentityId(), msg.getSsk(), msgTracker,
					IdentityRequest.SuccessAction.REQUEST_MESSAGE,
					msgRequest);
		}

		try {
			queueFcpRequest(request);
		} catch (FcpException e) {
			LOG.log(Level.WARNING, "Failed to add message request", e);
		}
	}
}
