package jfms.util;

import java.util.concurrent.atomic.AtomicInteger;

public class RequestID {
	private final String prefix;
	private final AtomicInteger nextID= new AtomicInteger();

	public RequestID(String prefix) {
		this.prefix = prefix;
	}

	public String getNext() {
		StringBuilder str = new StringBuilder(prefix);
		str.append(nextID.getAndIncrement());

		return str.toString();
	}
}
