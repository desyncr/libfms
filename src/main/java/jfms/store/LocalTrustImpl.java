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
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.FmsManager;
import jfms.fms.IdentityManager;
import jfms.fms.Trust;

public class LocalTrustImpl {
	private static final Logger LOG = Logger.getLogger(LocalTrustImpl.class.getName());

	static Trust getLocalTrust(JDBCWrapper jdbcWrapper,
			int localIdentityId, int identityId) {

		final String selectTrust = "SELECT trustlist_trust, message_trust, "
			+ "trustlist_trust_comment, message_trust_comment "
			+ "FROM local_trust "
			+ "WHERE local_identity_id=? AND identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectTrust,
				s -> handleGetLocalTrust(s, localIdentityId, identityId),
				null);
	}

	static Trust handleGetLocalTrust(PreparedStatement pstmt,
			int localIdentityId, int identityId) throws SQLException {

		Trust trust = null;
		IdentityManager identityManager = FmsManager.getInstance().getIdentityManager();
		pstmt.setInt(1, localIdentityId);
		pstmt.setInt(2, identityId);

		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			trust = new Trust();

			String ssk = identityManager.getSsk(identityId);
			if (ssk == null) {
				LOG.log(Level.WARNING, "skipping unknown ID {0}",
						identityId);
				return null;
			}
			trust.setIdentity(ssk);

			int trustListTrust = rs.getInt(1);
			if (!rs.wasNull()) {
				trust.setTrustListTrustLevel(trustListTrust);
			}

			int messageTrust = rs.getInt(2);
			if (!rs.wasNull()) {
				trust.setMessageTrustLevel(messageTrust);
			}

			trust.setTrustListTrustComment(rs.getString(3));
			trust.setMessageTrustComment(rs.getString(4));
		}

		return trust;
	}

	static Map<Integer,Integer> getLocalTrustListTrusts(
			JDBCWrapper jdbcWrapper, int localIdentityId, int minTrust) {

		final String selectTrusts = "SELECT identity_id, trustlist_trust "
			+ "FROM local_trust "
			+ "WHERE local_identity_id=? AND trustlist_trust>=?";

		return jdbcWrapper.executePreparedStatement(selectTrusts,
				s -> handleGetLocalTrustListTrusts(s, localIdentityId, minTrust),
				Collections.emptyMap());
	}

	static Map<Integer,Integer> handleGetLocalTrustListTrusts(
			PreparedStatement pstmt,
			int localIdentityId, int minTrust) throws SQLException {

		Map<Integer, Integer> trusts = new HashMap<>();
		pstmt.setInt(1, localIdentityId);
		pstmt.setInt(2, minTrust);

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			int identityId = rs.getInt(1);
			int trustListTrust = rs.getInt(2);
			trusts.put(identityId, trustListTrust);
		}

		return trusts;
	}

	static Map<Integer,Integer> getLocalMessageTrusts(
			JDBCWrapper jdbcWrapper, int localIdentityId) {

		final String selectTrusts = "SELECT identity_id, message_trust "
			+ "FROM local_trust "
			+ "WHERE local_identity_id=? AND message_trust IS NOT NULL";

		return jdbcWrapper.executePreparedStatement(selectTrusts,
				s -> handleGetLocalMessageTrusts(s, localIdentityId),
				Collections.emptyMap());
	}

	static Map<Integer,Integer> handleGetLocalMessageTrusts(
			PreparedStatement pstmt, int localIdentityId) throws SQLException {

		Map<Integer, Integer> trusts = new HashMap<>();

		pstmt.setInt(1, localIdentityId);

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			int identityId = rs.getInt(1);
			int messageTrust = rs.getInt(2);
			trusts.put(identityId, messageTrust);
		}

		return trusts;
	}

	static List<Trust> getLocalTrustList(JDBCWrapper jdbcWrapper,
			int localIdentityId) {

		final String selectTrust = "SELECT identity_id, "
			+ "trustlist_trust, message_trust, "
			+ "trustlist_trust_comment, message_trust_comment "
			+ "FROM local_trust "
			+ "WHERE local_identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectTrust,
				s -> handleGetLocalTrustList(s, localIdentityId),
				Collections.emptyList());
	}

	static List<Trust> handleGetLocalTrustList(PreparedStatement pstmt,
			int localIdentityId) throws SQLException {

		List<Trust> trustList = new ArrayList<>();
		IdentityManager identityManager = FmsManager.getInstance().getIdentityManager();

		pstmt.setInt(1, localIdentityId);

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			Trust trust = new Trust();

			int identityId = rs.getInt(1);
			String ssk = identityManager.getSsk(identityId);
			if (ssk == null) {
				LOG.log(Level.WARNING, "skipping unknown ID {0}",
						identityId);
				continue;
			}
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

	static boolean saveSeedTrust(JDBCWrapper jdbcWrapper,
			Integer localIdentityId) {

		final String insertSeedTrust = "INSERT INTO local_trust"
			+ "(local_identity_id, identity_id, "
			+ "trustlist_trust, trustlist_trust_comment) "
			+ "SELECT ?, id.identity_id, ?, ? "
			+ "FROM identity id "
			+ "WHERE added_by=?";

		return jdbcWrapper.executePreparedStatement(insertSeedTrust,
				(PreparedStatement pstmt) -> {
					pstmt.setInt(1, localIdentityId);
					pstmt.setInt(2, Constants.DEFAULT_SEED_TRUST);
					pstmt.setString(3, "Seed Identity");
					pstmt.setInt(4, Constants.ADD_SEED_IDENTITY);
					pstmt.executeUpdate();

					return true;
				},
				false);
	}

	static boolean migrateSeedTrust(JDBCWrapper jdbcWrapper,
			int newTrustListTrust) {

		final String updateSeedTrust = "UPDATE local_trust "
			+ "SET trustlist_trust=? "
			+ "WHERE EXISTS (SELECT * FROM identity i "
			+ "WHERE i.identity_id = local_trust.identity_id AND "
			+ "i.added_by=? AND local_trust.trustlist_trust=50)";

		return jdbcWrapper.executePreparedStatement(updateSeedTrust,
				(PreparedStatement pstmt) -> {
					pstmt.setInt(1, newTrustListTrust);
					pstmt.setInt(2, Constants.ADD_SEED_IDENTITY);
					pstmt.executeUpdate();

					return true;
				},
				false);
	}

	static boolean updateLocalTrust(JDBCWrapper jdbcWrapper,
			Integer localIdentityId, Integer identityId, Trust trust) {

		return jdbcWrapper.executeTransaction(
			w -> handleUpdateLocalTrust(w, localIdentityId, identityId, trust),
			false);
	}

	static boolean handleUpdateLocalTrust(JDBCWrapper jdbcWrapper,
			int localIdentityId, Integer identityId, Trust trust)
		throws SQLException {

		final String updateTrust = "UPDATE local_trust "
			+ "SET trustlist_trust=?, message_trust=?, "
			+ "trustlist_trust_comment=?, message_trust_comment=? "
			+ "WHERE local_identity_id=? AND identity_id=?";
		final String insertTrust = "INSERT INTO local_trust"
			+ "(local_identity_id, identity_id, "
			+ "trustlist_trust, message_trust, "
			+ "trustlist_trust_comment, message_trust_comment) "
			+ "VALUES(?,?,?,?,?,?)";

		// TODO remove entry if all values are zero

		int rowCount;
		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(updateTrust)) {
			if (trust.getTrustListTrustLevel() >= 0) {
				pstmt.setInt(1, trust.getTrustListTrustLevel());
			} else {
				pstmt.setNull(1, Types.INTEGER);
			}
			if (trust.getMessageTrustLevel() >= 0) {
				pstmt.setInt(2, trust.getMessageTrustLevel());
			} else {
				pstmt.setNull(2, Types.INTEGER);
			}
			final String trustListComment = trust.getTrustListTrustComment();
			if (trustListComment != null && !trustListComment.isEmpty()) {
				pstmt.setString(3, trustListComment);
			} else {
				pstmt.setString(3, null);
			}
			final String msgComment = trust.getMessageTrustComment();
			if (msgComment != null && !msgComment.isEmpty()) {
				pstmt.setString(4, msgComment);
			} else {
				pstmt.setString(4, null);
			}
			pstmt.setInt(5, localIdentityId);
			pstmt.setInt(6, identityId);

			rowCount = pstmt.executeUpdate();
		}

		if (rowCount == 0 && (trust.getTrustListTrustLevel() >= 0 ||
					trust.getMessageTrustLevel() >= 0)) {
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertTrust)) {
				pstmt.setInt(1, localIdentityId);
				pstmt.setInt(2, identityId);
				if (trust.getTrustListTrustLevel() >= 0) {
					pstmt.setInt(3, trust.getTrustListTrustLevel());
				} else {
					pstmt.setNull(3, Types.INTEGER);
				}
				if (trust.getMessageTrustLevel() >= 0) {
					pstmt.setInt(4, trust.getMessageTrustLevel());
				} else {
					pstmt.setNull(4, Types.INTEGER);
				}
				final String trustListComment = trust.getTrustListTrustComment();
				if (trustListComment != null && !trustListComment.isEmpty()) {
					pstmt.setString(5, trustListComment);
				} else {
					pstmt.setString(5, null);
				}
				final String msgComment = trust.getMessageTrustComment();
				if (msgComment != null && !msgComment.isEmpty()) {
					pstmt.setString(6, msgComment);
				} else {
					pstmt.setString(6, null);
				}

				pstmt.executeUpdate();
			}
		}

		return true;
	}

	private LocalTrustImpl() {
	}
}
