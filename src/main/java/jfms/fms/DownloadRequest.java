package jfms.fms;

import java.util.List;

public abstract class DownloadRequest  {
	private String id;
	private String key;
	private DownloadRequest chainedRequest;
	private int ttl;

	public DownloadRequest(String id, String key) {
		this.id = id;
		this.key = key;
		this.ttl = -1;
	}

	public DownloadRequest(String id, String key, int ttl) {
		this.id = id;
		this.key = key;
		this.ttl = ttl;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	public List<String> getAdditionalFields() {
		return null;
	}

	public int getTTL() {
		return ttl;
	}

	public void setTTL(int ttl) {
		this.ttl = ttl;
	}

	public DownloadRequest getChainedRequest() {
		return chainedRequest;
	}

	public void setChainedRequest(DownloadRequest chainedRequest) {
		this.chainedRequest = chainedRequest;
	}

	public String getNextId() {
		final String currentId = getId();
		String mainId;
		int subId;
		int dotIndex = currentId.indexOf('.');
		if (dotIndex >= 0) {
			mainId = currentId.substring(0, dotIndex);
			subId = Integer.parseInt(currentId.substring(dotIndex + 1));
		} else {
			mainId = currentId;
			subId = 0;
		}

		StringBuilder str = new StringBuilder(mainId);
		str.append('.');
		str.append(++subId);

		return str.toString();
	}

	public int isSuccessful() {
		return -1;
	}

	public abstract void finished(byte[] data);

	public boolean redirect(String redirectURI) {
		return false;
	}

	public void error(int code) {
	}
}
