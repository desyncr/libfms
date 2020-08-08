package jfms.fms;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IdentityIntroduction {
	private static final Logger LOG = Logger.getLogger(IdentityIntroduction.class.getName());

	private LocalDate date;
	private String uuid;
	private String solution;

	public static String calculateSolutionHash(String solution) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			byte[] sha1 = md.digest(solution.getBytes(StandardCharsets.ISO_8859_1));

			StringBuilder str = new StringBuilder();
			for (byte b : sha1) {
				str.append(charForDigit((b&0xff)>>4));
				str.append(charForDigit(b&0x0f));
			}

			return str.toString();
		} catch (NoSuchAlgorithmException e) {
			// we should never get here because SHA-1 support is required
			LOG.log(Level.SEVERE, "failed to calculate SHA-1", e);
			return null;
		}
	}

	private static char charForDigit(int digit) {
		if (digit < 10) {
			return (char)('0' + digit);
		} else {
			return (char)('A' + digit - 10);
		}
	}

	public LocalDate getDate() {
		return date;
	}

	public void setDate(LocalDate date) {
		this.date = date;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUUID(String uuid) {
		this.uuid = uuid;
	}

	public String getSolution() {
		return solution;
	}

	public void setSolution(String solution) {
		this.solution = solution;
	}

	public String getSolutionHash() {
		return calculateSolutionHash(solution);
	}
}
