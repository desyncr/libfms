package jfms.fms;

import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jfms.config.Constants;

public class Sanitizer {
	private static final Logger LOG = Logger.getLogger(Sanitizer.class.getName());
	private static final Pattern CTRL_PATTERN = Pattern.compile("\\p{Cntrl}+");
	private static final Pattern WS_PATTERN = Pattern.compile("[\\h\\v]");

	public static final String sanitizeName(String name) {
		return sanitizeString("name", name, Constants.MAX_NAME_LENGTH);
	}

	public static final String sanitizeTrustComment(String comment) {
		return sanitizeString("comment",
				comment, Constants.MAX_TRUST_COMMENT_LENGTH);
	}

	public static final String sanitizeSubject(String subject) {
		return sanitizeString("subject", subject, Constants.MAX_SUBJECT_LENGTH);
	}

	public static final String sanitizeSignature(String signature) {
		if (signature.length() > Constants.MAX_SIGNATURE_LENGTH) {
			return signature.substring(0, Constants.MAX_SIGNATURE_LENGTH);
		} else {
			return signature;
		}
	}

	public static final int sanitzeTrustLevel(String value) {
		if (value == null) {
			return -1;
		}

		int level;
		try {
			level = Integer.parseInt(value);
			if (level < 0 || level > 100) {
				level = -1;
			}
		} catch (NumberFormatException e) {
			level = -1;
		}

		if (level == -1) {
			LOG.log(Level.INFO, "Removing invalid trust level {0}", value);
		}

		return level;
	}

	public static final String sanitizeBoard(String board) {
		// File Formats specification:
		// All board names must be lower case and no longer than 40 characters.
		// Commas and whitespace characters are not allowed in board names.

		// Use sanitation logic similar to the reference implementation

		// remove ASCII control characters
		Matcher m1 = CTRL_PATTERN.matcher(board);
		String sanitized = m1.replaceAll("");

		// remove comma and everything after comma
		int commaIndex = sanitized.indexOf(',');
		if (commaIndex >= 0) {
			sanitized = sanitized.substring(0, commaIndex);
		}

		// replace whitespace by underscore
		Matcher m2 = WS_PATTERN.matcher(sanitized);
		sanitized = m2.replaceAll("_");

		// remove leading underscores
		int firstChar;
		for (firstChar=0; firstChar<sanitized.length(); firstChar++) {
			if (sanitized.charAt(firstChar) != '_') {
				break;
			}
		}

		if (firstChar > 0) {
			sanitized = sanitized.substring(firstChar);
		}

		// limit to 40 characters
		if (sanitized.length() > Constants.MAX_BOARD_LENGTH) {
			sanitized = sanitized.substring(0, Constants.MAX_BOARD_LENGTH);
		}

		sanitized = sanitized.toLowerCase(Locale.ENGLISH);
		if (!board.equals(sanitized)) {
			LOG.log(Level.INFO, "Sanitized board {0} -> {1}", new Object[]{
					escapeString(board), escapeString(sanitized)});
		}

		return sanitized;
	}

	public static String escapeString(String input)
	{
		StringBuilder out = new StringBuilder();
		for (int i = 0; i < input.length(); i++) {
			final char c = input.charAt(i);
			if (c < 0x21 || c > 0x7e) {
				out.append("\\u");
				out.append(Integer.toHexString(c));
			} else {
				out.append(c);
			}
		}

		return out.toString();
	}

	private static String sanitizeString(String field,
			String value, int maxLength) {

		Matcher m = CTRL_PATTERN.matcher(value);
		String sanitized = m.replaceAll("");

		if (sanitized.length() > maxLength) {
			sanitized = sanitized.substring(0, maxLength);
		}

		if (!value.equals(sanitized)) {
			LOG.log(Level.INFO, "Sanitized {0} {1} -> {2}", new Object[]{
					field, escapeString(value), escapeString(sanitized)});
		}

		return sanitized;
	}

	private Sanitizer() {
	}
}
