package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JfmsInfoImpl {
	static String getValue(JDBCWrapper jdbcWrapper, String key) {
		final String query = "SELECT value FROM jfms_info "
			+ "WHERE key=?";

		return jdbcWrapper.executePreparedStatement(query,
				rs -> handleGetValue(rs, key),
				null);
	}

	static String handleGetValue(PreparedStatement pstmt, String key)
		throws SQLException {

		pstmt.setString(1, key);
		ResultSet rs = pstmt.executeQuery();

		String value = null;
		if (rs.next()) {
			value = rs.getString(1);
		}

		return value;
	}

	static boolean saveValue(JDBCWrapper jdbcWrapper, String key, String value) {

		return jdbcWrapper.executeTransaction(
				w -> handleSaveValue(w, key, value),
				false);
	}

	static boolean handleSaveValue(JDBCWrapper jdbcWrapper,
			String key, String value) throws SQLException {

		final String updateQuery = "UPDATE jfms_info "
			+ "SET value=? "
			+ "WHERE key=?";
		final String insertQuery = "INSERT INTO jfms_info "
			+ "(key, value) "
			+ "VALUES (?,?)";

		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(updateQuery)) {
			pstmt.setString(1, value);
			pstmt.setString(2, key);
			int rowCount = pstmt.executeUpdate();
			if (rowCount != 0) {
				return true;
			}
		}

		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertQuery)) {
			pstmt.setString(1, key);
			pstmt.setString(2, value);
			pstmt.executeUpdate();
		}

		return true;
	}

	private JfmsInfoImpl() {
	}
}
