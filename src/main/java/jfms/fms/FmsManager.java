package jfms.fms;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fcp.FcpClient;
import jfms.fcp.FcpStatusListener;
import jfms.store.Store;

public class FmsManager implements FcpStatusListener {
	private static final Logger LOG = Logger.getLogger(FmsManager.class.getName());
	private static final FmsManager instance = new FmsManager();

	private Store store;
	private final BoardManager boardManager;
	private final IdentityManager identityManager;
	private final MessageManager messageManager;
	private final TrustManager trustManager;
	private FcpClient fcpClient;
	private boolean isOffline;
	private ScheduledThreadPoolExecutor pool;
	private ProgressListener progressListener;
	private FcpStatusListener statusListener;
	private InsertThread insertThread;

	public static FmsManager getInstance() {
		return instance;
	}

	private FmsManager() {
		boardManager = new BoardManager();
		identityManager = new IdentityManager();
		messageManager = new MessageManager();
		trustManager = new TrustManager();
	}

	public void initializeStore() throws SQLException {
		if (store == null) {
			store = new Store(Constants.DATABASE_URL);
		}
	}

	public void initialize(List<String> seedIdentities, String fcpHost, int fcpPort, boolean isOffline) throws SQLException {
	    this.isOffline = isOffline;
		fcpClient = new FcpClient("jfms", fcpHost, fcpPort);
		fcpClient.setStatusListener(this);
		if (store == null) {
			store = new Store(Constants.DATABASE_URL);
		}
		store.initialize(seedIdentities);
		boardManager.initialize();
		identityManager.initialize();
		trustManager.initialize();
	}

	public void initialize() throws SQLException {
		initialize(null, Constants.DEFAULT_FCP_HOST, Integer.parseInt(Constants.DEFAULT_FCP_PORT), false);
	}

	public synchronized void run() {
		if (!isOffline) {
			startBackgroundThread();
		}
	}

	public synchronized void shutdown() {
		shutdown(true);
	}

	@Override
	public synchronized void statusChanged(FcpClient.Status status) {
		if (status != FcpClient.Status.CONNECTED) {
			LOG.log(Level.INFO, "FCP disconnected, stopping tasks...");
			shutdown(false);
			if (!isOffline) {
				startBackgroundThread();
			}
		}

		if (statusListener != null) {
			statusListener.statusChanged(status);
		}
	}

	public synchronized void setOffline(boolean isOffline) {
		this.isOffline = isOffline;

		if (isOffline) {
			shutdown(true);
		} else {
			startBackgroundThread();
		}
	}

	public synchronized boolean isOffline() {
		return isOffline;
	}

	/*
	 * Set store.
	 * Only use for test code. Production code should use initialize().
	 */
	public void setStore(Store store) {
		this.store = store;
	}

	public Store getStore() {
		return store;
	}

	public BoardManager getBoardManager() {
		return boardManager;
	}

	public IdentityManager getIdentityManager() {
		return identityManager;
	}

	public MessageManager getMessageManager() {
		return messageManager;
	}

	public TrustManager getTrustManager() {
		return trustManager;
	}

	public void setProgressListener(ProgressListener listener) {
		this.progressListener = listener;
	}

	public synchronized void setFcpStatusListener(FcpStatusListener listener) {
		this.statusListener = listener;
	}

	public synchronized Future<?> startPuzzleThread(LocalDate date,
			PuzzleListener listener) {

		if (pool == null) {
			LOG.log(Level.WARNING, "Thread pool not avaialble");
			return null;
		}

		PuzzleThread puzzleThread = new PuzzleThread(fcpClient, date);
		puzzleThread.addListener(listener);

		return pool.submit(puzzleThread);
	}

	public synchronized Future<?> startKeyPairGeneratorThread(
			LocalIdentity localIdentity,
			Consumer<Boolean> keyGeneratedCallback) {

		if (pool == null) {
			LOG.log(Level.WARNING, "Thread pool not avaialble");
			return null;
		}

		KeyGeneratorThread keygenThread =
				new KeyGeneratorThread(fcpClient, keyGeneratedCallback);
		keygenThread.setLocalIdentity(localIdentity);

		return pool.submit(keygenThread);
	}

	public InsertThread getInsertThread() {
		return insertThread;
	}


	private void startBackgroundThread() {
		if (pool != null) {
			LOG.log(Level.WARNING, "Thread pool already exists");
			return;
		}

		pool = new ScheduledThreadPoolExecutor(4);
		pool.setRemoveOnCancelPolicy(true);

		DownloadThread downloadThread = new DownloadThread(fcpClient);
		if (progressListener != null) {
			downloadThread.setProgressListener(progressListener);
		}
		pool.scheduleWithFixedDelay(downloadThread,
				Constants.STARTUP_IDLE_TIME,
				Constants.DOWNLOAD_IDLE_TIME,
				TimeUnit.SECONDS);

		insertThread = new InsertThread(fcpClient);
		pool.scheduleWithFixedDelay(insertThread,
				Constants.STARTUP_IDLE_TIME,
				Constants.INSERT_IDLE_TIME,
				TimeUnit.SECONDS);
	}

	private void shutdown(boolean awaitTermination) {
		LOG.log(Level.INFO, "Shutting down...");
		if (pool != null) {
			pool.shutdownNow();
		}

		if (fcpClient != null) {
			fcpClient.shutdown();
		}

		if (awaitTermination) {
			try {
				// blocks UI if termination fails; limit to 5 seconds
				if (pool != null && !pool.awaitTermination(5, TimeUnit.SECONDS)) {
					LOG.log(Level.INFO, "Failed to terminate threads");
				}
			} catch (InterruptedException e) {
				LOG.log(Level.INFO, "Failed terminate threads: interrupted", e);
			}
		}

		pool = null;
		insertThread = null;
		DownloadThread.resetIteration();
	}
}
