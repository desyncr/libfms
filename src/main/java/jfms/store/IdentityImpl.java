package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.AddedInfo;
import jfms.fms.Identity;

public class IdentityImpl {
	private static final Logger LOG = Logger.getLogger(IdentityImpl.class.getName());
	private static final String INSERT_IDENTITY = "INSERT INTO identity "
			+ "(ssk, date_added, added_by) "
			+ "VALUES(?,?,?)";
	private static final AddedInfo FALLBACK_ADDED_INFO = new AddedInfo(
			Constants.FALLBACK_DATE,
			Constants.ADD_UNKNOWN);

	static Map<Integer, Identity> getIdentities(JDBCWrapper jdbcWrapper) {
		final String selectIdentities = "SELECT "
			+ "identity_id, ssk, name, signature, avatar, single_use, "
			+ "publish_trustlist, publish_boardlist, freesite_edition "
			+ "FROM identity";

		return jdbcWrapper.executeStatement(selectIdentities,
				IdentityImpl::handleGetIdentities,
				Collections.emptyMap());
	}

	static Map<Integer, Identity> handleGetIdentities(ResultSet rs)
		throws SQLException {

		Map<Integer, Identity> identities = new HashMap<>();

		while (rs.next()) {
			Identity id = new Identity();
			int identityId = rs.getInt(1);
			id.setSsk(rs.getString(2));
			id.setName(rs.getString(3));

			id.setSignature(rs.getString(4));
			id.setAvatar(rs.getString(5));
			id.setSingleUse(rs.getBoolean(6));
			id.setPublishTrustList(rs.getBoolean(7));
			id.setPublishBoardList(rs.getBoolean(8));

			int freesiteEdition = rs.getInt(9);
			if (rs.wasNull()) {
				freesiteEdition = -1;
			}
			id.setFreesiteEdition(freesiteEdition);

			identities.put(identityId, id);
		}

		return identities;
	}

	static boolean updateIdentity(JDBCWrapper jdbcWrapper, int identityId,
			Identity identity) {

		final String updateIdentity = "UPDATE identity SET "
			+ "name=?, "
			+ "signature=?, "
			+ "avatar=?, "
			+ "single_use=?, "
			+ "publish_trustlist=?, "
			+ "publish_boardlist=?, "
			+ "freesite_edition=? "
			+ "WHERE identity_id=?";

		return jdbcWrapper.executePreparedStatement(updateIdentity,
				s -> handleUpdateIdentity(s, identityId, identity),
				false);
	}

	static boolean handleUpdateIdentity(PreparedStatement pstmt,
			int identityId, Identity identity) throws SQLException {

		pstmt.setString(1, identity.getName());
		pstmt.setString(2, identity.getSignature());
		pstmt.setString(3, identity.getAvatar());
		pstmt.setBoolean(4, identity.getSingleUse());
		pstmt.setBoolean(5, identity.getPublishTrustList());
		pstmt.setBoolean(6, identity.getPublishBoardList());

		final int freesiteEdition = identity.getFreesiteEdition();
		if (freesiteEdition >= 0) {
			pstmt.setInt(7, freesiteEdition);
		} else {
			pstmt.setNull(7, Types.INTEGER);
		}

		pstmt.setInt(8, identityId);

		int rows = pstmt.executeUpdate();
		if (rows != 1) {
			LOG.log(Level.WARNING, "identity update affected {0} rows",
					rows);
			return false;
		}

		return true;
	}

	static List<Integer> getActiveIdentities(JDBCWrapper jdbcWrapper,
			LocalDate activeSince, LocalDate singleUseAddedSince) {

		final String selectIdentities = "SELECT i.identity_id "
			+ "FROM identity i "
			+ "LEFT OUTER JOIN request_history h ON (i.identity_id = h.identity_id) "
			+ "WHERE (h.last_identity_date >= ? OR "
			+ "(h.last_identity_date IS NULL AND h.last_fail_date IS NULL)) "
			+ "AND (i.single_use=0 OR (i.single_use=1 AND i.date_added >= ?)) "
			+ "ORDER BY last_identity_date DESC";

		return jdbcWrapper.executePreparedStatement(selectIdentities,
				s -> handleGetActiveIdentities(s, activeSince, singleUseAddedSince),
				Collections.emptyList());
	}

	static List<Integer> handleGetActiveIdentities(PreparedStatement pstmt,
			LocalDate activeSince, LocalDate singleUseAddedSince)
		throws SQLException {

			List<Integer> identities = new ArrayList<>();

			pstmt.setString(1, Utils.format(activeSince));
			pstmt.setString(2, Utils.format(singleUseAddedSince));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				identities.add(rs.getInt(1));
			}

			return identities;
	}

	static List<Integer> getInactiveIdentities(JDBCWrapper jdbcWrapper,
			LocalDate inactiveSince, LocalDate notFailedSince,
			LocalDate singleUseAddedSince) {

		final String selectIdentities = "SELECT i.identity_id "
			+ "FROM identity i "
			+ "INNER JOIN request_history h ON (i.identity_id = h.identity_id) "
			+ "WHERE ((h.last_identity_date IS NULL OR h.last_identity_date < ?) AND "
			+ "(h.last_fail_date IS NULL OR h.last_fail_date < ?) AND "
			+ "(h.last_identity_date IS NOT NULL OR h.last_fail_date IS NOT NULL)) "
			+ "AND (i.single_use=0 OR (i.single_use=1 AND i.date_added >= ?)) "
			+ "ORDER BY last_identity_date DESC";

		return jdbcWrapper.executePreparedStatement(selectIdentities,
				s -> handleGetInactiveIdentities(s, inactiveSince, notFailedSince, singleUseAddedSince),
				Collections.emptyList());
	}

	static List<Integer> handleGetInactiveIdentities(PreparedStatement pstmt,
			LocalDate inactiveSince, LocalDate notFailedSince,
			LocalDate singleUseAddedSince) throws SQLException {

		List<Integer> identities = new ArrayList<>();

		pstmt.setString(1, Utils.format(inactiveSince));
		pstmt.setString(2, Utils.format(notFailedSince));
		pstmt.setString(3, Utils.format(singleUseAddedSince));
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			identities.add(rs.getInt(1));
		}

		return identities;
	}

	static List<String> getSeedIdentitySsks(JDBCWrapper jdbcWrapper) {
		final String selectIdentities = "SELECT ssk "
			+ "FROM identity "
			+ "WHERE added_by=?";

		return jdbcWrapper.executePreparedStatement(selectIdentities,
				IdentityImpl::handleGetSeedIdentitySsks,
				Collections.emptyList());
	}

	static List<String> handleGetSeedIdentitySsks(PreparedStatement pstmt)
		throws SQLException {

		List<String> ssks = new ArrayList<>();

		pstmt.setInt(1, Constants.ADD_SEED_IDENTITY);
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			ssks.add(rs.getString(1));
		}

		return ssks;
	}

	static Set<String> getRecentSsks(JDBCWrapper jdbcWrapper,
			LocalDate fromDate) {

		final String selectIdentities = "SELECT ssk "
			+ "FROM identity i "
			+ "INNER JOIN request_history h ON (i.identity_id = h.identity_id) "
			+ "WHERE last_identity_date >= ?";

		return jdbcWrapper.executePreparedStatement(selectIdentities,
				s -> handleGetRecentSsks(s, fromDate),
				Collections.emptySet());
	}

	static Set<String> handleGetRecentSsks(PreparedStatement pstmt,
			LocalDate fromDate) throws SQLException {

		Set<String> ssks = new HashSet<>();

		pstmt.setString(1, Utils.format(fromDate));
		ResultSet results = pstmt.executeQuery();
		while (results.next()) {
			ssks.add(results.getString(1));
		}

		return ssks;
	}

	static AddedInfo getAddedInfo(JDBCWrapper jdbcWrapper, int identityId) {
		final String selectIdentities= "SELECT date_added, added_by "
			+ "FROM identity "
			+ "WHERE identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectIdentities,
				s -> handleGetAddedInfo(s, identityId),
				FALLBACK_ADDED_INFO);
	}

	static AddedInfo handleGetAddedInfo(PreparedStatement pstmt,
			int identityId) throws SQLException {

		AddedInfo addedInfo = FALLBACK_ADDED_INFO;

		pstmt.setInt(1, identityId);
		ResultSet rs = pstmt.executeQuery();
		if(rs.next()) {
			String dateStr = rs.getString(1);
			int addedBy = rs.getInt(2);

			if (dateStr != null && !rs.wasNull()) {
				addedInfo = new AddedInfo(Utils.date(dateStr), addedBy);
			}
		}

		return addedInfo;
	}

	static int saveIdentity(JDBCWrapper jdbcWrapper,
			int trusterId, String ssk, LocalDate date) {

		return jdbcWrapper.executePreparedStatement(INSERT_IDENTITY,
				s -> handleSaveIdentity(s, trusterId, ssk, date),
				-1);
	}

	static int saveIdentityInternal(JDBCWrapper jdbcWrapper,
			int trusterId, String ssk) throws SQLException {
		return jdbcWrapper.executePreparedStatement(INSERT_IDENTITY,
				s -> handleSaveIdentity(s, trusterId, ssk, LocalDate.now()));
	}

	static int handleSaveIdentity(PreparedStatement pstmt,
			int trusterId, String ssk, LocalDate date) throws SQLException {

		pstmt.setString(1, ssk);
		pstmt.setString(2, Utils.format(date));
		pstmt.setInt(3, trusterId);
		pstmt.executeUpdate();

		Integer identityId = null;
		ResultSet rs = pstmt.getGeneratedKeys();
		if (rs.next()) {
			identityId = rs.getInt(1);
		}

		if (identityId == null) {
			throw new SQLException("failed to get ID for identity " + ssk);
		}

		return identityId;
	}

	private IdentityImpl() {
	}
}
