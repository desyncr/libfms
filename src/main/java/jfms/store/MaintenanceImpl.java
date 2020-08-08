package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

public class MaintenanceImpl {
	static int countRemovableIdentities(JDBCWrapper jdbcWrapper,
			LocalDate date, boolean inactiveOnly) {

		StringBuilder query = new StringBuilder();
		query.append("SELECT COUNT(*) FROM (SELECT i.identity_id "
			+ "FROM identity i "
			+ "LEFT OUTER JOIN request_history h ON i.identity_id = h.identity_id "
			+ "LEFT OUTER JOIN message m ON i.identity_id = m.identity_id "
			+ "WHERE i.date_added < ? ");

		if (inactiveOnly) {
			query.append("AND (h.last_identity_date IS NULL OR h.last_identity_date < ?) ");
		}

		query.append("GROUP BY i.identity_id, i.date_added, h.last_identity_date "
			+ "HAVING COUNT(m.message_id) = 0)");

		return jdbcWrapper.executePreparedStatement(query.toString(),
				s -> handleCountRemovableIdentities(s, date, inactiveOnly),
				0);
	}

	static int handleCountRemovableIdentities(PreparedStatement pstmt,
			LocalDate date, boolean inactiveOnly) throws SQLException {

		final String removeBefore = Utils.format(date.minusDays(20));
		pstmt.setString(1, removeBefore);
		if (inactiveOnly) {
			pstmt.setString(2, removeBefore);
		}

		int count = 0;
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			count = rs.getInt(1);
		}

		return count;
	}

	static boolean removeIdentities(JDBCWrapper jdbcWrapper,
			LocalDate date, boolean inactiveOnly) {

		return jdbcWrapper.executeTransaction(
				w -> handleRemoveIdentities(w, date, inactiveOnly),
				false);
	}

	static boolean handleRemoveIdentities(JDBCWrapper jdbcWrapper,
			LocalDate date, boolean inactiveOnly) throws SQLException {

		final String removeBefore = Utils.format(date.minusDays(20));

		StringBuilder query = new StringBuilder();
		query.append("DELETE FROM identity WHERE identity_id IN (SELECT i.identity_id "
			+ "FROM identity i "
			+ "LEFT OUTER JOIN request_history h ON i.identity_id = h.identity_id "
			+ "LEFT OUTER JOIN message m ON i.identity_id = m.identity_id "
			+ "WHERE i.date_added < ? ");

		if (inactiveOnly) {
			query.append("AND (h.last_identity_date IS NULL OR h.last_identity_date < ?) ");
		}

		query.append("GROUP BY i.identity_id, i.date_added, h.last_identity_date "
			+ "HAVING COUNT(m.message_id) = 0)");

		final String deleteLocalTrust = "DELETE FROM local_trust "
			+ "WHERE NOT EXISTS (SELECT * FROM identity i "
			+ "WHERE i.identity_id = local_trust.identity_id)";
		final String deletePeeerTrust = "DELETE FROM peer_trust "
			+ "WHERE NOT EXISTS (SELECT * FROM identity i "
			+ "WHERE i.identity_id = peer_trust.target_identity_id)";

		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(query.toString())) {
			pstmt.setString(1, removeBefore);
			pstmt.executeUpdate();
		}

		try (Statement stmt = jdbcWrapper.createStatement(deleteLocalTrust)) {
			stmt.executeUpdate(deleteLocalTrust);
		}

		try (Statement stmt = jdbcWrapper.createStatement(deletePeeerTrust)) {
			stmt.executeUpdate(deletePeeerTrust);
		}

		return true;
	}

	private MaintenanceImpl() {
	}
}
