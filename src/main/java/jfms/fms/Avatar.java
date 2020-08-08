package jfms.fms;

import java.time.LocalDate;

public class Avatar {
	private final int identityId;
	private final String key;
	private final int tries;
	private final LocalDate lastFailDate;

	public Avatar(int identityId, String key, int tries, LocalDate lastFailDate) {
		this.identityId = identityId;
		this.key = key;
		this.tries = tries;
		this.lastFailDate = lastFailDate;
	}

	public static String getExtension(String filename) {
		String ext = null;

		// javafx.scene.image.Image supports the following formats:
		// * BMP
		// * GIF
		// * JPEG
		// * PNG
		String name = filename.toLowerCase();
		if (name.endsWith(".bmp")) {
			ext = "bmp";
		} else if (name.endsWith(".gif")) {
			ext = "gif";
		} else if (name.endsWith(".jpg") || name.endsWith(".jpeg")) {
			ext = "jpg";
		} else if (name.endsWith(".png")) {
			ext = "png";
		}

		return ext;
	}

	public int getIdentityId() {
		return identityId;
	}

	public String getKey() {
		return key;
	}

	public int getTries() {
		return tries;
	}

	public LocalDate getLastFailDate() {
		return lastFailDate;
	}
}
