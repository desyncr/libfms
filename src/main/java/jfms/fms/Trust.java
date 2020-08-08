package jfms.fms;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Trust {
	private static final Logger LOG = Logger.getLogger(Trust.class.getName());

	private int identityId;
	private String identity;
	private int messageTrustLevel = -1;
	private int trustListTrustLevel = -1;
	private String messageTrustComment;
	private String trustListTrustComment;

	public Trust() {
	}

	public Trust(int identityId) {
		this.identityId = identityId;
	}

	public Trust(String ssk) {
		this.identity = ssk;
	}

	public void logChanges(Trust t) {
		if (!Objects.equals(identity, t.identity)) {
			LOG.log(Level.FINEST, "identity: {0} -> {1}", new Object[]{
				identity, t.identity});
		}

		if (messageTrustLevel != t.messageTrustLevel) {
			LOG.log(Level.FINEST,
					"message trust level: {0} -> {1}", new Object[]{
					messageTrustLevel, t.messageTrustLevel});
		}

		if (trustListTrustLevel != t.trustListTrustLevel) {
			LOG.log(Level.FINEST,
					"trustlist trust level: {0} -> {1}", new Object[]{
					trustListTrustLevel, t.trustListTrustLevel});
		}

		if (!Objects.equals(messageTrustComment, t.messageTrustComment)) {
			LOG.log(Level.FINEST,
					"message trust comment: {0} -> {1}", new Object[]{
					messageTrustComment, t.messageTrustComment});
		}

		if (!Objects.equals(trustListTrustComment, t.trustListTrustComment)) {
			LOG.log(Level.FINEST,
					"trustlist trust comment: {0} -> {1}", new Object[]{
					trustListTrustComment, t.trustListTrustComment});
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}

		if (!(o instanceof Trust)) {
			return false;
		}

		final Trust t = (Trust)o;

		return
			Objects.equals(identity, t.identity) &&
			messageTrustLevel == t.messageTrustLevel &&
			trustListTrustLevel == t.trustListTrustLevel &&
			Objects.equals(messageTrustComment, t.messageTrustComment) &&
			Objects.equals(trustListTrustComment, t.trustListTrustComment);
	}

	@Override
	public int hashCode() {
		return
			13 * Objects.hashCode(identity) +
			17 * messageTrustLevel +
			19 * trustListTrustLevel +
			23 * Objects.hashCode(messageTrustComment) +
			29 * Objects.hashCode(trustListTrustComment);
	}

	public int getIdentityId() {
		return identityId;
	}

	public void setIdentityId(int identityId) {
		this.identityId = identityId;
	}

	public String getIdentity() {
		return identity;
	}

	public void setIdentity(String identity) {
		this.identity = identity;
	}

	public void setMessageTrustLevel(int messageTrustLevel) {
		this.messageTrustLevel = messageTrustLevel;
	}

	public int getMessageTrustLevel() {
		return messageTrustLevel;
	}

	public void setTrustListTrustLevel(int trustListTrustLevel) {
		this.trustListTrustLevel = trustListTrustLevel;
	}

	public int getTrustListTrustLevel() {
		return trustListTrustLevel;
	}

	public void setMessageTrustComment(String messageTrustComment) {
		this.messageTrustComment = messageTrustComment;
	}

	public String getMessageTrustComment() {
		return messageTrustComment;
	}

	public void setTrustListTrustComment(String trustListTrustComment) {
		this.trustListTrustComment = trustListTrustComment;
	}

	public String getTrustListTrustComment() {
		return trustListTrustComment;
	}
}
