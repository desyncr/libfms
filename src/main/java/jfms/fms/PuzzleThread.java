package jfms.fms;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fcp.FcpClient;
import jfms.fcp.FcpException;
import jfms.fcp.FcpListener;
import jfms.util.RequestID;
import jfms.util.RequestLimiter;

public class PuzzleThread implements FcpListener, Runnable {
	private static final Logger LOG = Logger.getLogger(PuzzleThread.class.getName());

	private final FcpClient fcpClient;
	private PuzzleListener listener;
	private final AtomicInteger foundPuzzles = new AtomicInteger();
	private final LocalDate date;
	private final RequestID requestID = new RequestID("puzzlerequest-");
	private final RequestLimiter requestLimiter = new RequestLimiter();
	private final AtomicBoolean running = new AtomicBoolean(false);

	private final Map<String, IntroductionPuzzleRequest> fcpRequests =
		new ConcurrentHashMap<>();

	public PuzzleThread(FcpClient fcpClient, LocalDate date) {
		this.fcpClient = fcpClient;
		this.date = date;
	}

	@Override
	public void run() {
		running.set(true);
		LOG.log(Level.FINE, "Started FMS puzzle thread with ID {0}",
				Thread.currentThread().getId());

		IdentityManager identityManager = FmsManager.getInstance().getIdentityManager();
		TrustManager trustManager = FmsManager.getInstance().getTrustManager();

		Set<Integer> trustlistTrustedIds = trustManager.getTrustListTrustedIds(
				Integer.parseInt(Constants.DEFAULT_MIN_PEER_MESSAGE_TRUST)
		);

		LOG.log(Level.INFO, "Found {0} trusted IDs", trustlistTrustedIds.size());

		try {
			List<IntroductionPuzzleRequest> requests = new ArrayList<>();
			for (int identityId : trustlistTrustedIds) {
				final Identity identity = identityManager.getIdentity(identityId);
				final String ssk = identity.getSsk();

				// only try to download puzzle if we have successfully
				// retrieved a trust list on the same day
				RequestTracker idTracker = RequestTracker.createSingleRequest(
						RequestType.TRUST_LIST, identityId, date);
				if (idTracker != null) {
					continue;
				}

				RequestTracker puzzleTracker = RequestTracker.create(date,
						Constants.MAX_INTRODUCTION_PUZZLE_INDEX);

				IntroductionPuzzleRequest puzzleRequest =
					new IntroductionPuzzleRequest(null,	ssk, puzzleTracker);
				requests.add(puzzleRequest);
			}

			LOG.log(Level.INFO, "Found {0} IDs for puzzle download",
					requests.size());

			if (requests.isEmpty()) {
				listener.onError("No puzzles available. Try again later.");
			} else {
				fcpClient.start();
			}

			for (IntroductionPuzzleRequest request : requests) {
				requestLimiter.waitUntilReady(
						Constants.MAX_CONCURRENT_PUZZLE_REQUESTS);
				final String key = request.getKey();
				request.setId(requestID.getNext());
				fcpRequests.put(request.getId(), request);
				fcpClient.requestKey(request.getId(), key, this, -1);

				requestLimiter.addRequest();

				// MAX_PUZZLE_REQUESTS is only an approximation
				// because we don't know if pending requests will succeed
				if (foundPuzzles.get() >= Constants.MAX_PUZZLE_REQUESTS) {
					break;
				}
			}

			LOG.log(Level.FINEST, "Waiting for FMS puzzle requests to finish");
			requestLimiter.waitUntilReady(1);

			if (!requests.isEmpty() && foundPuzzles.get() == 0) {
				listener.onError("Failed to download puzzles. Try again later.");
			}
		} catch (InterruptedException e) {
			LOG.log(Level.FINE, "FMS puzzle thread interrupted");
		} catch (Exception e) {
			LOG.log(Level.WARNING, "exception in FMS puzzle thread", e);
		}

		LOG.log(Level.FINE, "FMS puzzle thread stopped");
		running.set(false);

		// in case the dialog was closed, cancel dangling requests
		try {
			for (String id : fcpRequests.keySet()) {
				fcpClient.cancel(id);
			}
		} catch (FcpException e) {
			LOG.log(Level.WARNING, "failed to cancel puzzle requests", e);
		}
	}

	@Override
	public void error(String fcpIdentifier, int code) {
		if (!running.get()) {
			LOG.log(Level.FINE, "got FCP response but thread not running");
			return;
		}

		IntroductionPuzzleRequest fcpRequest = fcpRequests.remove(fcpIdentifier);
		if (fcpRequest != null) {
			notifyListener(fcpRequest.getPuzzle(), fcpRequest.getSsk());
		} else {
			LOG.log(Level.WARNING, "got FCP response for unknown ID: {0}", fcpIdentifier);
		}

		requestLimiter.requestDone();
	}

	@Override
	public void finished(String fcpIdentifier, byte[] data) {
		if (!running.get()) {
			LOG.log(Level.FINE, "got FCP response but thread not running");
			return;
		}

		IntroductionPuzzleRequest fcpRequest = fcpRequests.remove(fcpIdentifier);
		if (fcpRequest == null ) {
			LOG.log(Level.WARNING, "got FCP response for unknown ID: {0}", fcpIdentifier);
			requestLimiter.requestDone();
			return;
		}

		fcpRequest.finished(data);

		IntroductionPuzzleRequest chainedRequest =
			(IntroductionPuzzleRequest)fcpRequest.getChainedRequest();
		if (chainedRequest != null) {
			try {
				LOG.log(Level.FINEST, "Got chained request!");

				final String id = chainedRequest.getId();
				fcpRequests.put(id, chainedRequest);
				fcpClient.requestKey(id, chainedRequest.getKey(), this, -1);

				requestLimiter.addRequest();
			} catch (FcpException e) {
				LOG.log(Level.WARNING, "Failed to handle FCP finished response", e);
			}
		} else {
			notifyListener(fcpRequest.getPuzzle(), fcpRequest.getSsk());
		}

		requestLimiter.requestDone();
	}

	@Override
	public void redirect(String fcpIdentifier, String redirectURI) {
	}

	@Override
	public void putSuccessful(String fcpIdentifier, String key) {
	}

	@Override
	public void keyPairGenerated(String fcpIdentifier, String publicKey,
			String privateKey) {
	}

	public void addListener(PuzzleListener listener) {
		this.listener = listener;
	}

	private void notifyListener(IntroductionPuzzle puzzle, String ssk) {
		if (puzzle != null && listener != null) {
			LOG.log(Level.FINEST, "found puzzle, notifying listener");

			String publisher = "";
			final IdentityManager identityManager =
				FmsManager.getInstance().getIdentityManager();
			Integer identityId = identityManager.getIdentityId(ssk);
			if (identityId != null) {
				Identity id = identityManager.getIdentity(identityId);
				if (id != null) {
					publisher = id.getFullName();
				}
			}

			listener.onPuzzleAdded(puzzle, publisher);
			foundPuzzles.incrementAndGet();
		} else {
			LOG.log(Level.FINEST, "No puzzle found");
		}
	}
}
