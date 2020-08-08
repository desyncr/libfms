package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jfms.fms.Avatar;

public class AvatarImpl {
	static boolean populateAvatarTable(JDBCWrapper jdbcWrapper) {
		return jdbcWrapper.executeTransaction(
				AvatarImpl::handlePopulateAvatarTable,
				false);
	}

	static boolean handlePopulateAvatarTable(JDBCWrapper jdbcWrapper)
		throws SQLException {

		// create missing avatar entries from existing identities
		final String selectAvatar = "SELECT i.identity_id, i.avatar "
			+ " FROM identity i "
			+ " LEFT OUTER JOIN avatar a ON (i.identity_id == a.identity_id) "
			+ " WHERE i.avatar IS NOT NULL AND a.identity_id IS NULL";

		List<Integer> missingAvatars = new ArrayList<>();
		try (Statement stmt = jdbcWrapper.createStatement(selectAvatar)) {
			ResultSet rs = stmt.executeQuery(selectAvatar);
			while (rs.next()) {
				int identityId = rs.getInt(1);
				String key = rs.getString(2);
				if (Avatar.getExtension(key) != null) {
					missingAvatars.add(identityId);
				}
			}
		}

		final String insertAvatar = "INSERT into avatar "
			+ "(identity_id, disabled, tries) "
			+ "VALUES(?,0,0)";
		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertAvatar)) {
			for (int id : missingAvatars) {
				pstmt.setInt(1, id);
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		}

		return true;
	}

	static boolean updateAvatar(JDBCWrapper jdbcWrapper, int identityId) {
		return jdbcWrapper.executeTransaction(
				w -> handleUpdateAvatar(w, identityId),
				false);
	}

	static boolean handleUpdateAvatar(JDBCWrapper jdbcWrapper, int identityId)
		throws SQLException {

		final String updateAvatar = "UPDATE avatar "
			+ "SET extension=NULL, tries=0, last_fail_date=NULL "
			+ "WHERE identity_id=?";
		final String insertAvatar = "INSERT INTO avatar "
			+ "(identity_id, disabled, tries) "
			+ "VALUES(?,0,0)";

		int rowCount;
		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(updateAvatar)) {
			pstmt.setInt(1, identityId);
			rowCount = pstmt.executeUpdate();
		}

		if (rowCount == 0) {
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertAvatar)) {
				pstmt.setInt(1, identityId);
				pstmt.executeUpdate();
			}
		}

		return true;
	}

	static String getAvatarExtension(JDBCWrapper jdbcWrapper, int identityId) {

		final String selectAvatar = "SELECT extension FROM avatar "
				+ "WHERE identity_id=? AND disabled <> 1";

		return jdbcWrapper.executePreparedStatement(selectAvatar,
				s -> handleGetAvatarExtension(s, identityId),
				null);
	}

	static String handleGetAvatarExtension(PreparedStatement pstmt,
			int identityId) throws SQLException {

		String extension = null;

		pstmt.setInt(1, identityId);
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			extension = rs.getString(1);
		}

		return extension;
	}

	static void setAvatarExtension(JDBCWrapper jdbcWrapper,
			int identityId, String extension) {

		final String updateAvatar = "UPDATE avatar "
			+ "SET extension=? "
			+ "WHERE identity_id=?";

		jdbcWrapper.executePreparedStatement(updateAvatar,
				(PreparedStatement pstmt) -> {
					pstmt.setString(1, extension);
					pstmt.setInt(2, identityId);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	static void setAvatarFailed(JDBCWrapper jdbcWrapper,
			int identityId, LocalDate failDate) {

		final String updateAvatar = "UPDATE avatar "
			+ "SET tries=tries+1, last_fail_date=? "
			+ "WHERE identity_id=?";

		jdbcWrapper.executePreparedStatement(updateAvatar,
				(PreparedStatement pstmt) -> {
					pstmt.setString(1, Utils.format(failDate));
					pstmt.setInt(2, identityId);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	static boolean removeAvatar(JDBCWrapper jdbcWrapper, int identityId) {
		final String removeAvatar = "DELETE FROM avatar WHERE identity_id=?";

		return jdbcWrapper.executePreparedStatement(removeAvatar,
				(PreparedStatement pstmt) -> {
					pstmt.setInt(1, identityId);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	static List<Avatar> getMissingAvatars(JDBCWrapper jdbcWrapper) {
		final String selectAvatar =
			"SELECT a.identity_id, i.avatar, a.tries, a.last_fail_date "
			+ "FROM avatar a "
			+ "INNER JOIN identity i ON (a.identity_id = i.identity_id) "
			+ "WHERE a.extension IS NULL AND a.disabled <> 1";

		return jdbcWrapper.executePreparedStatement(selectAvatar,
				AvatarImpl::handleGetMissingAvatars,
				Collections.emptyList());
	}

	static List<Avatar> handleGetMissingAvatars(PreparedStatement pstmt)
		throws SQLException {

		List<Avatar> avatars= new ArrayList<>();

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			String dateStr = rs.getString(4);
			LocalDate lastFailDate;
			if (dateStr != null) {
				lastFailDate = Utils.date(dateStr);
			} else {
				lastFailDate = null;
			}
			Avatar avatar = new Avatar(
				rs.getInt(1),
				rs.getString(2),
				rs.getInt(3),
				lastFailDate);
			avatars.add(avatar);
		}

		return avatars;
	}

	static boolean isAvatarDisabled(JDBCWrapper jdbcWrapper, int identityId) {
		final String selectAvatar =
			"SELECT disabled FROM avatar WHERE identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectAvatar,
			s -> handleIsAvatarDisabled(s, identityId), false);
	}

	static boolean handleIsAvatarDisabled(PreparedStatement pstmt,
		int identityId) throws SQLException {

		boolean disabled = false;
		pstmt.setInt(1, identityId);
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			disabled = rs.getBoolean(1);
		}

		return disabled;
	}

	static void setAvatarDisabled(JDBCWrapper jdbcWrapper,
			int identityId, boolean enabled) {

		final String updateAvatar = "UPDATE avatar "
			+ "SET disabled=? "
			+ "WHERE identity_id=?";

		jdbcWrapper.executePreparedStatement(updateAvatar,
				(PreparedStatement pstmt) -> {
					pstmt.setBoolean(1, enabled);
					pstmt.setInt(2, identityId);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	private AvatarImpl() {
	}
}
