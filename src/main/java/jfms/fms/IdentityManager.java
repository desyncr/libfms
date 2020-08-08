package jfms.fms;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.imageio.ImageIO;

import jfms.config.Constants;
import jfms.store.Store;

public class IdentityManager {
	private static final Logger LOG = Logger.getLogger(IdentityManager.class.getName());

	private Map<String, Integer> sskToIdentityId;
	private Map<Integer, Identity> identities = new HashMap<>();

	public void initialize() {
		identities = FmsManager.getInstance().getStore().getIdentities();
		sskToIdentityId = identities.entrySet().stream().collect(
				Collectors.toMap(e -> e.getValue().getSsk(), Map.Entry::getKey));
		LOG.log(Level.FINEST,
				"IdentityManager initialized with {0,number,0} identities",
				identities.size());
	}

	public int size() {
		return identities.size();
	}

	public Integer getIdentityId(String ssk) {
		return sskToIdentityId.get(ssk);
	}

	public void addManualIdentity(String ssk) {
		if (sskToIdentityId.containsKey(ssk)) {
			LOG.log(Level.FINEST, "Skipping {0}: already exists", ssk);
			return;
		}

		LOG.log(Level.FINE, "Adding identity {0} (manual)", ssk);
		final Store store = FmsManager.getInstance().getStore();
		int identityId = store.saveIdentity(Constants.ADD_MANUALLY, ssk);
		if (identityId != -1) {
			addIdentity(identityId, ssk);
		}
	}

	public void addIdentityFromPuzzle(String ssk) {
		if (sskToIdentityId.containsKey(ssk)) {
			LOG.log(Level.FINEST, "Skipping {0}: already exists", ssk);
			return;
		}

		LOG.log(Level.FINE, "Adding identity {0} (puzzle solved)", ssk);
		final Store store = FmsManager.getInstance().getStore();
		int identityId = store.saveIdentity(Constants.ADD_PUZZLE_SOLVED, ssk);
		if (identityId != -1) {
			addIdentity(identityId, ssk);
		}
	}

	public void addIdentityFromTrustList(int identityId, String ssk) {
		if (identities.containsKey(identityId)) {
			LOG.log(Level.FINEST, "Skipping {0}: already exists", ssk);
			return;
		}

		LOG.log(Level.FINE, "Adding identity {0} from trust list", ssk);
		addIdentity(identityId, ssk);
	}

	public boolean updateIdentity(int identityId, Identity newIdentity) {
		final Identity oldIdentity = identities.get(identityId);
		if (oldIdentity.equals(newIdentity)) {
			return false;
		}

		LOG.log(Level.FINEST, "updating Identity of {0}",
				oldIdentity.getFullName());

		Store store = FmsManager.getInstance().getStore();
		store.updateIdentity(identityId, newIdentity);

		identities.put(identityId, newIdentity);

		// update avatar table and avatar disk cache
		final String newAvatar = newIdentity.getAvatar();
		if (!Objects.equals(oldIdentity.getAvatar(), newAvatar)) {
			String oldExtension = store.getAvatarExtension(identityId);

			if (newAvatar != null && Avatar.getExtension(newAvatar) != null) {
				store.updateAvatar(identityId);
			} else {
				store.removeAvatar(identityId);
			}

			if (oldExtension != null) {
				deleteAvatar(identityId, oldExtension);
			}
		}

		return true;
	}

	public String getSsk(int identityId) {
		Identity id = identities.get(identityId);
		if (id != null) {
			return id.getSsk();
		} else {
			return null;
		}
	}

	public Identity getIdentity(int identityId) {
		return identities.get(identityId);
	}

	public Map<Integer, Identity> getIdentities() {
		return identities;
	}

	public byte[] getAvatar(int identityId) {
		final Identity id = identities.get(identityId);
		if (id == null) {
			return null;
		}

		if (id.getAvatar() == null) {
			return null;
		}

		final Store store = FmsManager.getInstance().getStore();
		String extension = store.getAvatarExtension(identityId);
		if (extension == null) {
			return null;
		}

		Path path = getAvatarPath(identityId, extension);
		byte[] data = null;
		try {
			data = Files.readAllBytes(path);
		} catch (IOException e) {
			LOG.log(Level.WARNING, "Failed to read avatar", e);

			store.removeAvatar(identityId);
		}

		return data;
	}

	public boolean saveAvatar(int identityId, String extension, byte[] data) {
		boolean success = false;
		try {
			// try to load image
			// if loading fails, assume it's not an image
			if (ImageIO.read(new ByteArrayInputStream(data)) == null) {
				LOG.log(Level.FINE, "avatar key does not contain a supported image");
				return false;
			}

			Files.createDirectories(Paths.get(Constants.AVATAR_DIR));
			Path path = getAvatarPath(identityId, extension);
			Files.write(path, data);
			success = true;
		} catch (IOException e) {
			LOG.log(Level.WARNING, "Failed to save avatar", e);
		}

		return success;
	}

	public void deleteAvatar(int identityId, String extension) {
		Path path = getAvatarPath(identityId, extension);
		try {
			Files.delete(path);
		} catch (IOException e) {
			LOG.log(Level.WARNING, "Failed to delete avatar", e);
		}
	}

	public int importLocalIdentities(List<LocalIdentity> localIdentities) {
		int importCount = 0;
		final Store store = FmsManager.getInstance().getStore();

		for (LocalIdentity identity : localIdentities) {
			identity.setIsActive(true);
			int identityId = store.saveLocalIdentity(identity);
			if (identityId != -1) {
				store.saveSeedTrust(identityId);

				if (Integer.parseInt(Constants.DEFAULT_DEFAULT_ID) <= 0) {
				}

				importCount++;
			}
		}

		return importCount;
	}

	private void addIdentity(int identityId, String ssk) {
		Identity id = new Identity(ssk);
		identities.put(identityId, id);
		sskToIdentityId.put(ssk, identityId);
	}

	private Path getAvatarPath(int identityId, String extension) {
		StringBuilder str = new StringBuilder();
		str.append(identityId);
		str.append('.');
		str.append(extension);

		return Paths.get(Constants.AVATAR_DIR, str.toString());
	}
}
