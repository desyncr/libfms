package jfms.util;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


public class RequestLimiter {
	private static final Logger LOG = Logger.getLogger(RequestLimiter.class.getName());

	private final Lock lock = new ReentrantLock();
	private final Condition cond = lock.newCondition();
	private volatile int pendingRequests = 0;

	public void addRequest() {
		lock.lock();
		try {
			pendingRequests++;
		} finally {
			lock.unlock();
		}
	}

	public void requestDone() {
		lock.lock();
		try {
			pendingRequests--;
			cond.signal();
		} finally {
			lock.unlock();
		}
	}

	public void waitUntilReady(int maxRequests) throws InterruptedException {
		lock.lock();
		try {
			while (pendingRequests >= maxRequests) {
				LOG.log(Level.FINEST, "requests pending: {0}/{1}", new Object[]{
						pendingRequests, maxRequests});
				cond.await();
			}
		} finally {
			lock.unlock();
		}
	}
}
