package jfms.fms;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import jfms.store.Store;

public class AvatarRequest extends DownloadRequest {
	private final String key;
	private final int identityId;
	private int successful = 0;

	public AvatarRequest(String id, int identityId, String key) {
		super(id, key);
		this.identityId = identityId;
		this.key = key;
	}

	@Override
	public List<String> getAdditionalFields() {
		return Arrays.asList("FilterData=true");
	}

	@Override
	public void finished(byte[] data) {
		IdentityManager identityManager = FmsManager.getInstance()
			.getIdentityManager();

		final String extension = Avatar.getExtension(key);
		boolean saved = identityManager.saveAvatar(identityId, extension, data);

		final Store store = FmsManager.getInstance().getStore();
		if (saved) {
			store.setAvatarExtension(identityId, extension);
		} else {
			store.setAvatarFailed(identityId, LocalDate.now());
		}

		successful = 1;
	}

	@Override
	public boolean redirect(String redirectURI) {
		setKey(redirectURI);

		return true;
	}

	@Override
	public void error(int code) {
		final Store store = FmsManager.getInstance().getStore();
		store.setAvatarFailed(identityId, LocalDate.now());
	}

	@Override
	public int isSuccessful() {
		return successful;
	}
}
