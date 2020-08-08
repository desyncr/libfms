package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import jfms.fms.FmsManager;
import jfms.fms.IdentityManager;
import jfms.fms.Trust;

public class PeerTrustImpl {
	private static final Logger LOG = Logger.getLogger(PeerTrustImpl.class.getName());
	private static final String SELECT_TRUST_LIST =
		"SELECT target_identity_id, "
		+ "trustlist_trust, message_trust, "
		+ "trustlist_trust_comment, message_trust_comment "
		+ "FROM peer_trust "
		+ "WHERE identity_id=?";


	static List<Trust> getTrustList(JDBCWrapper jdbcWrapper,
			int identityId) {

		return jdbcWrapper.executePreparedStatement(SELECT_TRUST_LIST,
			s -> handleGetTrustList(s, identityId),
			Collections.emptyList());
	}

	static List<Trust> getTrustListInternal(JDBCWrapper jdbcWrapper,
			int identityId) throws SQLException {

		return jdbcWrapper.executePreparedStatement(SELECT_TRUST_LIST,
			s -> handleGetTrustList(s, identityId));
	}

	static List<Trust> handleGetTrustList(PreparedStatement pstmt,
			int identityId) throws SQLException {

		IdentityManager identityManager = FmsManager.getInstance().getIdentityManager();
		List<Trust> trustList = new ArrayList<>();

		pstmt.setInt(1, identityId);
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			int targetIdentityId = rs.getInt(1);
			String ssk = identityManager.getSsk(targetIdentityId);
			if (ssk == null) {
				LOG.log(Level.WARNING, "skipping unknown ID {0}",
						targetIdentityId);
				continue;
			}

			Trust trust = new Trust();
			trust.setIdentityId(targetIdentityId);
			trust.setIdentity(ssk);

			int trustListTrust = rs.getInt(2);
			if (!rs.wasNull()) {
				trust.setTrustListTrustLevel(trustListTrust);
			}

			int messageTrust = rs.getInt(3);
			if (!rs.wasNull()) {
				trust.setMessageTrustLevel(messageTrust);
			}

			trust.setTrustListTrustComment(rs.getString(4));
			trust.setMessageTrustComment(rs.getString(5));

			trustList.add(trust);
		}

		return trustList;
	}

	static List<Trust> getNumericTrustList(JDBCWrapper jdbcWrapper,
			int identityId) {

		final String selectTrusts = "SELECT target_identity_id, "
			+ "trustlist_trust, message_trust, "
			+ "trustlist_trust_comment, message_trust_comment "
			+ "FROM peer_trust "
			+ "WHERE identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectTrusts,
				s -> handleGetNumericTrustList(s, identityId),
				Collections.emptyList());
	}

	static List<Trust> getNumericTrusteeList(
			JDBCWrapper jdbcWrapper, int targetIdentityId) {

		final String selectTrusts = "SELECT identity_id, "
			+ "trustlist_trust, message_trust, "
			+ "trustlist_trust_comment, message_trust_comment "
			+ "FROM peer_trust "
			+ "WHERE target_identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectTrusts,
				s -> handleGetNumericTrustList(s, targetIdentityId),
				Collections.emptyList());
	}

	static List<Trust> handleGetNumericTrustList(PreparedStatement pstmt,
			int identityId) throws SQLException {

		List<Trust> trustList = new ArrayList<>();

		pstmt.setInt(1, identityId);

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			Trust trust = new Trust(rs.getInt(1));

			int trustListTrust = rs.getInt(2);
			if (!rs.wasNull()) {
				trust.setTrustListTrustLevel(trustListTrust);
			}

			int messageTrust = rs.getInt(3);
			if (!rs.wasNull()) {
				trust.setMessageTrustLevel(messageTrust);
			}

			trust.setTrustListTrustComment(rs.getString(4));
			trust.setMessageTrustComment(rs.getString(5));

			trustList.add(trust);
		}

		return trustList;
	}

	static Map<Integer, String> saveTrustList(JDBCWrapper jdbcWrapper,
			int trusterId, List<Trust> trustList) {

		return jdbcWrapper.executeTransaction(
				w -> handleSaveTrustList(w, trusterId, trustList),
				null);
	}

	static Map<Integer, String> handleSaveTrustList(JDBCWrapper jdbcWrapper,
			int trusterId, List<Trust> trustList) throws SQLException {

		LOG.log(Level.FINEST, "Processing trust list of ID {0}", trusterId);

		final IdentityManager identityManager =
			FmsManager.getInstance().getIdentityManager();

		Map<Integer, String> newIdentities = null;

		// fill in numeric identity IDs
		for (Trust t : trustList) {
			final String ssk = t.getIdentity();
			Integer identityId = identityManager.getIdentityId(ssk);
			if (identityId == null) {
				identityId = IdentityImpl.saveIdentityInternal(jdbcWrapper,
						trusterId, ssk);

				if (newIdentities == null) {
					newIdentities = new HashMap<>();
				}
				newIdentities.put(identityId, ssk);
			}

			t.setIdentityId(identityId);
		}

		List<Trust> currentTrustList = getTrustListInternal(jdbcWrapper,
				trusterId);

		Map<Integer, Trust> currentTrustMap = currentTrustList
			.stream()
			.collect(Collectors.toMap(Trust::getIdentityId, t->t));

		List<Trust> newTrusts = new ArrayList<>();
		List<Trust> changedTrusts = new ArrayList<>();
		Set<Integer> removedTrusts = currentTrustList
			.stream()
			.map(Trust::getIdentityId)
			.collect(Collectors.toSet());



		for (Trust t : trustList) {
			// don't jfms.store empty trust entries
			if (t.getTrustListTrustLevel() < 0 && t.getMessageTrustLevel() < 0) {
				continue;
			}

			Trust currentTrust = currentTrustMap.get(t.getIdentityId());
			if (currentTrust != null) {
				if (!currentTrust.equals(t)) {
					if (LOG.isLoggable(Level.FINEST)) {
						LOG.log(Level.FINEST, "Trust of {0} changed",
								t.getIdentity());
						currentTrust.logChanges(t);
					}
					changedTrusts.add(t);
				}
			} else {
				newTrusts.add(t);
			}
			removedTrusts.remove(t.getIdentityId());
		}

		removePeerTrustEntries(jdbcWrapper, trusterId, removedTrusts);
		addPeerTrustEntries(jdbcWrapper, trusterId, newTrusts);
		updatePeerTrustEntries(jdbcWrapper, trusterId, changedTrusts);

		return newIdentities;
	}

	private static void removePeerTrustEntries(JDBCWrapper jdbcWrapper,
			int trusterId, Set<Integer> trustsToRemove) throws SQLException {

		final String deletePeerTrust = "DELETE FROM peer_trust "
			+ "WHERE identity_id=? AND target_identity_id=?";

		IdentityManager identityManager = FmsManager.getInstance().getIdentityManager();

		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(deletePeerTrust)) {
			pstmt.setInt(1, trusterId);
			for (int targetIdentityId : trustsToRemove) {
				LOG.log(Level.FINEST, "removing peer trust entry for {0}",
						identityManager.getSsk(targetIdentityId));

				pstmt.setInt(2, targetIdentityId);
				pstmt.addBatch();
			}

			pstmt.executeBatch();
		}
	}

	private static void addPeerTrustEntries(JDBCWrapper jdbcWrapper,
			int trusterId, List<Trust> newTrusts) throws SQLException {

		final String insertTrust = "INSERT INTO peer_trust "
			+ "(identity_id, target_identity_id, trustlist_trust, message_trust, trustlist_trust_comment, message_trust_comment) "
			+ "VALUES(?,?,?,?,?,?)";

		try (PreparedStatement statement = jdbcWrapper.prepareStatement(insertTrust)) {
			statement.setInt(1, trusterId);
			for (Trust t : newTrusts) {
				LOG.log(Level.FINEST, "adding new peer trust entry for {0}",
						t.getIdentity());

				statement.setInt(2, t.getIdentityId());
				if (t.getTrustListTrustLevel() >= 0) {
					statement.setInt(3, t.getTrustListTrustLevel());
				} else {
					statement.setNull(3, Types.INTEGER);
				}
				if (t.getMessageTrustLevel() >= 0) {
					statement.setInt(4, t.getMessageTrustLevel());
				} else {
					statement.setNull(4, Types.INTEGER);
				}
				final String trustListComment = t.getTrustListTrustComment();
				if (trustListComment != null && !trustListComment.isEmpty()) {
					statement.setString(5, trustListComment);
				} else {
					statement.setString(5, null);
				}
				final String msgComment = t.getMessageTrustComment();
				if (msgComment != null && !msgComment.isEmpty()) {
					statement.setString(6, msgComment);
				} else {
					statement.setString(6, null);
				}

				statement.addBatch();
			}

			statement.executeBatch();
		}
	}

	private static void updatePeerTrustEntries(JDBCWrapper jdbcWrapper,
			int trusterId, List<Trust> changedTrusts) throws SQLException {

		final String updatePeerTrust = "UPDATE peer_trust "
			+ "SET trustlist_trust=?, message_trust=?, "
			+ "trustlist_trust_comment=?, message_trust_comment=? "
			+ "WHERE identity_id=? AND target_identity_id=?";

		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(updatePeerTrust)) {
			for (Trust t : changedTrusts) {
				LOG.log(Level.FINEST, "updating peer trust entry for {0}",
						t.getIdentity());

				if (t.getTrustListTrustLevel() >= 0) {
					pstmt.setInt(1, t.getTrustListTrustLevel());
				} else {
					pstmt.setNull(1, Types.INTEGER);
				}
				if (t.getMessageTrustLevel() >= 0) {
					pstmt.setInt(2, t.getMessageTrustLevel());
				} else {
					pstmt.setNull(2, Types.INTEGER);
				}
				final String trustListComment = t.getTrustListTrustComment();
				if (trustListComment != null && !trustListComment.isEmpty()) {
					pstmt.setString(3, trustListComment);
				} else {
					pstmt.setString(3, null);
				}
				final String msgComment = t.getMessageTrustComment();
				if (msgComment != null && !msgComment.isEmpty()) {
					pstmt.setString(4, msgComment);
				} else {
					pstmt.setString(4, null);
				}

				pstmt.setInt(5, trusterId);
				pstmt.setInt(6, t.getIdentityId());

				pstmt.addBatch();
			}

			pstmt.executeBatch();
		}
	}

	static Map<Integer, Map<Integer, Integer>> getPeerTrusts(JDBCWrapper jdbcWrapper) {

		final String selectTrusts =
			"SELECT i.identity_id, t.target_identity_id, t.trustlist_trust "
			+ "FROM peer_trust t "
			+ "INNER JOIN identity i ON (t.identity_id = i.identity_id) "
			+ "WHERE i.publish_trustlist=1 AND t.trustlist_trust IS NOT NULL";

		return jdbcWrapper.executePreparedStatement(selectTrusts,
				PeerTrustImpl::handleGetPeerTrusts,
				Collections.emptyMap());
	}

	static Map<Integer, Map<Integer, Integer>> handleGetPeerTrusts(
			PreparedStatement pstmt) throws SQLException {

		Map<Integer, Map<Integer, Integer>> result = new HashMap<>();

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			int trusteeId = rs.getInt(1);
			int targetId = rs.getInt(2);
			int trust = rs.getInt(3);

			Map<Integer, Integer> targetIdTrusts = result.computeIfAbsent(
					targetId, k -> new HashMap<>());
			targetIdTrusts.put(trusteeId, trust);
		}

		return result;
	}

	private PeerTrustImpl() {
	}
}
