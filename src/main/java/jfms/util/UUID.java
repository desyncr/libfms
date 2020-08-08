package jfms.util;

import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * FMS UUID.
 * UUID should begin with a standard UUID then a '@' and then the first part
 * of the inserting identity's SSK (the part between the '@' and first ',')
 * with '~' and '-' removed.
 */
public class UUID {
	private static final Logger LOG = Logger.getLogger(UUID.class.getName());
	private static final Pattern UUIDREPLACE_PATTERN = Pattern.compile("[~-]");

	public static String randomUUID(String ssk) {
		StringBuilder uuid = new StringBuilder();
		uuid.append(java.util.UUID.randomUUID().toString()
				.toUpperCase(Locale.ENGLISH));
		uuid.append('@');
		uuid.append(UUIDREPLACE_PATTERN.matcher(ssk.substring(4,47)).replaceAll(""));
		return uuid.toString();
	}

	public static boolean check(String ssk, String uuid) {
		final String receivedHash = uuid.substring(37);
		final String publicKeyHash = ssk.substring(4, 47);
		final String expectedHash = UUIDREPLACE_PATTERN
			.matcher(publicKeyHash).replaceAll("");
		if (!receivedHash.equals(expectedHash)) {
			LOG.log(Level.FINEST, "public key of message UUID does not match. "
			 + "expected: {0} received: {1}", new Object[]{
			 expectedHash, receivedHash});
			return false;
		}

		return true;
	}

	private UUID() {
	}
}
