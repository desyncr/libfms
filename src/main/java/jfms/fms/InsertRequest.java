package jfms.fms;

import java.time.LocalDate;

public class InsertRequest {
	private final RequestType type;
	private final LocalDate date;
	private final int index;
	private final int localIdentityId;
	private Object clientData;

	public InsertRequest(RequestType type, int localIdentityId, LocalDate date,
			int index) {
		this.type = type;
		this.date = date;
		this.index = index;
		this.localIdentityId = localIdentityId;
	}

	public RequestType getType() {
		return type;
	}

	public LocalDate getLocalDate() {
		return date;
	}

	public int getIndex() {
		return index;
	}

	public int getLocalIdentityId() {
		return localIdentityId;
	}

	public Object getClientData() {
		return clientData;
	}

	public void setClientData(Object clientData) {
		this.clientData = clientData;
	}
}
