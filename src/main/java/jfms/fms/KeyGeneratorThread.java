package jfms.fms;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.fcp.FcpClient;
import jfms.fcp.FcpListener;
import jfms.fcp.KeyPair;
import jfms.store.Store;

public class KeyGeneratorThread implements Runnable, FcpListener {
	private static final Logger LOG = Logger.getLogger(KeyGeneratorThread.class.getName());

	private final FcpClient fcpClient;
	private final Lock lock = new ReentrantLock();
	private final Condition cond = lock.newCondition();
	private volatile KeyPair keyPair;
	private LocalIdentity localIdentity;
	private final Consumer<Boolean> keyGeneratedCallback;

	public KeyGeneratorThread(FcpClient fcpClient,
			Consumer<Boolean> keyGeneratedCallback) {

		this.fcpClient = fcpClient;
		this.keyGeneratedCallback = keyGeneratedCallback;
	}

	public void setLocalIdentity(LocalIdentity localIdentity) {
		this.localIdentity = localIdentity;
	}

	@Override
	public void run() {
		LOG.log(Level.FINE, "Started KeyGenerator thread with ID {0}",
				Thread.currentThread().getId());
		LOG.log(Level.FINE, "Started KeyGenerator thread with Name {0}",
				Thread.currentThread().getName());

		try {
			fcpClient.start();

			lock.lock();
			try {
				fcpClient.generateKeyPair("keypair", this);

				while (keyPair == null) {
					cond.await();
				}
			} finally {
				lock.unlock();
			}

			LOG.log(Level.FINEST, "keypair generation finished");
			localIdentity.setSsk(keyPair.getPublicKey());
			localIdentity.setPrivateSsk(keyPair.getPrivateKey());

			final Store store = FmsManager.getInstance().getStore();
			store.saveLocalIdentity(localIdentity);
		} catch (InterruptedException e) {
			LOG.log(Level.FINE, "KeyGenerator thread interrupted");
		} catch (Exception e) {
			LOG.log(Level.WARNING, "exception in KeyGenerator thread", e);
		}

		if (keyGeneratedCallback != null) {
			keyGeneratedCallback.accept(keyPair != null);
		}

		LOG.log(Level.FINE, "KeyGenerator thread stopped");
	}

	@Override
	public void error(String fcpIdentifier, int code) {
	}

	@Override
	public void finished(String fcpIdentifier, byte[] data) {
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

		lock.lock();
		try {
			keyPair = new KeyPair(publicKey, privateKey);
			cond.signal();
		} finally {
			lock.unlock();
		}
	}
}
