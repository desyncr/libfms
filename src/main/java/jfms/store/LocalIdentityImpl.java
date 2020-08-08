package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.LocalIdentity;

public class LocalIdentityImpl {
	private static final Logger LOG = Logger.getLogger(LocalIdentityImpl.class.getName());

	static Map<Integer, LocalIdentity> retrieveLocalIdentities(
			JDBCWrapper jdbcWrapper) {

		final String selectIdentities ="SELECT local_identity_id, "
			+ "ssk, private_ssk, name, signature, avatar, single_use, "
			+ "publish_trustlist, publish_boardlist, active, creation_date "
			+ "FROM local_identity";

		return jdbcWrapper.executeStatement(selectIdentities,
			LocalIdentityImpl::handleRetrieveLocalIdentities,
			Collections.emptyMap());
	}

	static Map<Integer, LocalIdentity> handleRetrieveLocalIdentities(
			ResultSet rs) throws SQLException {

		Map<Integer, LocalIdentity> identities = new HashMap<>();

		while (rs.next()) {
			LocalIdentity id = new LocalIdentity();
			int identityId = rs.getInt(1);
			id.setSsk(rs.getString(2));
			id.setPrivateSsk(rs.getString(3));
			id.setName(rs.getString(4));
			id.setSignature(rs.getString(5));
			id.setAvatar(rs.getString(6));
			id.setSingleUse(rs.getBoolean(7));
			id.setPublishTrustList(rs.getBoolean(8));
			id.setPublishBoardList(rs.getBoolean(9));
			id.setIsActive(rs.getBoolean(10));

			String dateStr = rs.getString(11);
			if (dateStr != null) {
				id.setCreationDate(Utils.date(dateStr));
			} else {
				id.setCreationDate(Constants.FALLBACK_DATE);
			}

			identities.put(identityId, id);
		}

		return identities;
	}

	static LocalIdentity retrieveLocalIdentity(
			JDBCWrapper jdbcWrapper, int localIdentityId) {

		final String selectIdentity = "SELECT ssk, private_ssk, name, "
			+ "signature, avatar, single_use, "
			+ "publish_trustlist, publish_boardlist, active, creation_date "
			+ "FROM local_identity "
			+ "WHERE local_identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectIdentity,
			w -> handleRetrieveLocalIdentity(w, localIdentityId), null);
	}

	static LocalIdentity handleRetrieveLocalIdentity(
		PreparedStatement pstmt, int localIdentityId) throws SQLException {

		LocalIdentity id = null;

		pstmt.setInt(1, localIdentityId);
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			id = new LocalIdentity();
			id.setSsk(rs.getString(1));
			id.setPrivateSsk(rs.getString(2));
			id.setName(rs.getString(3));
			id.setSignature(rs.getString(4));
			id.setAvatar(rs.getString(5));
			id.setSingleUse(rs.getBoolean(6));
			id.setPublishTrustList(rs.getBoolean(7));
			id.setPublishBoardList(rs.getBoolean(8));
			id.setIsActive(rs.getBoolean(9));

			String dateStr = rs.getString(10);
			if (dateStr != null) {
				id.setCreationDate(Utils.date(dateStr));
			} else {
				id.setCreationDate(Constants.FALLBACK_DATE);
			}
		}

		return id;
	}

	static int saveLocalIdentity(JDBCWrapper jdbcWrapper,
			LocalIdentity identity) {

		return jdbcWrapper.executeTransaction(
			w -> handleSaveLocalIdentity(w, identity),
			-1);
	}

	static int handleSaveLocalIdentity(JDBCWrapper jdbcWrapper,
			LocalIdentity identity) throws SQLException {

		final String selectIdentity = "SELECT COUNT(*) FROM local_identity "
			+ "WHERE ssk=?";
		final String insertIdentity = "INSERT INTO local_identity "
			+ "(ssk, private_ssk, name, signature, avatar, single_use, "
			+ "publish_trustlist, publish_boardlist, active, creation_date) "
			+ "VALUES(?,?,?,?,?,?,?,?,?,?)";

		final LocalDate now = LocalDate.now(ZoneOffset.UTC);
		int identityId = -1;

		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(selectIdentity)) {
			pstmt.setString(1, identity.getSsk());
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				int idCount = rs.getInt(1);
				if (idCount > 0) {
					LOG.log(Level.FINE, "identity already exists");
					return -1;
				}
			}
		}

		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertIdentity)) {
			pstmt.setString(1, identity.getSsk());
			pstmt.setString(2, identity.getPrivateSsk());
			pstmt.setString(3, identity.getName());
			pstmt.setString(4, identity.getSignature());
			pstmt.setString(5, identity.getAvatar());
			pstmt.setBoolean(6, identity.getSingleUse());
			pstmt.setBoolean(7, identity.getPublishTrustList());
			pstmt.setBoolean(8, identity.getPublishBoardList());
			pstmt.setBoolean(9, identity.getIsActive());
			pstmt.setString(10, Utils.format(now));

			pstmt.executeUpdate();

			ResultSet rs = pstmt.getGeneratedKeys();
			if (rs.next()) {
				identityId = rs.getInt(1);
			}
			return identityId;
		}
	}

	static boolean updateLocalIdentity(JDBCWrapper jdbcWrapper,
			LocalIdentity localIdentity) {

		final String updateLocalIdentity = "UPDATE local_identity "
			+ "SET name=?, signature=?, avatar=?, single_use=?, "
			+ "publish_trustlist=?, active=? "
			+ "WHERE ssk=?";

		return jdbcWrapper.executePreparedStatement(updateLocalIdentity,
			s -> handleUpdateLocalIdentity(s, localIdentity),
			false);
	}

	static boolean handleUpdateLocalIdentity(PreparedStatement pstmt,
			LocalIdentity localIdentity) throws SQLException {

		pstmt.setString(1, localIdentity.getName());
		pstmt.setString(2, localIdentity.getSignature());
		pstmt.setString(3, localIdentity.getAvatar());
		pstmt.setBoolean(4, localIdentity.getSingleUse());
		pstmt.setBoolean(5, localIdentity.getPublishTrustList());
		pstmt.setBoolean(6, localIdentity.getIsActive());
		pstmt.setString(7, localIdentity.getSsk());

		int rows = pstmt.executeUpdate();

		return rows > 0;
	}

	static boolean setLocalIdentityActive(JDBCWrapper jdbcWrapper,
			int localIdentityId, boolean active) {

		final String updateLocalIdentity = "UPDATE local_identity "
			+ "SET active=? "
			+ "WHERE local_identity_id=?";

		return jdbcWrapper.executePreparedStatement(updateLocalIdentity,
			s -> handleSetLocalIdentityActive(s, localIdentityId, active),
			false);
	}

	static boolean handleSetLocalIdentityActive(PreparedStatement pstmt,
			int localIdentityId, boolean active) throws SQLException {

		pstmt.setBoolean(1, active);
		pstmt.setInt(2, localIdentityId);

		int rows = pstmt.executeUpdate();

		return rows > 0;
	}

	static boolean deleteLocalIdentity(JDBCWrapper jdbcWrapper,
			int localIdentityId) {

		return jdbcWrapper.executeTransaction(
			w -> handleDeleteLocalIdentity(w, localIdentityId),
			false);
	}

	static boolean handleDeleteLocalIdentity(JDBCWrapper jdbcWrapper,
			int localIdentityId) throws SQLException {

		final String[] deleteQueries = new String[] {
			"DELETE FROM local_identity WHERE local_identity_id=?",
			"DELETE FROM local_identity_insert WHERE local_identity_id=?",
			"DELETE FROM local_message WHERE local_identity_id=?",
			"DELETE FROM local_trust WHERE local_identity_id=?",
			"DELETE FROM messagelist_insert WHERE local_identity_id=?",
			"DELETE FROM trustlist_insert WHERE local_identity_id=?",
			"DELETE FROM identity_introduction_insert WHERE local_identity_id=?",
			"DELETE FROM introduction_puzzle WHERE local_identity_id=?"
		};

		for (String q : deleteQueries) {
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(q)) {
				pstmt.setInt(1, localIdentityId);
				pstmt.executeUpdate();
			}
		}

		return true;
	}

	private LocalIdentityImpl() {
	}
}
