package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import jfms.config.Constants;
import jfms.fms.DateIndex;
import jfms.fms.RequestType;

public class RequestHistoryImpl {
	public static final DateIndex FALLBACK_DATE_INDEX = new DateIndex(
			Constants.FALLBACK_DATE, -1);

	static DateIndex getLastRequestDateIndex(JDBCWrapper jdbcWrapper,
			Integer identityId, RequestType type) {

		final String selectHistory = "SELECT "
			+ getHistoryColumnName(type, "date") + ","
			+ getHistoryColumnName(type, "index") + " "
			+ "FROM request_history "
			+ "WHERE identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectHistory,
				s -> handleGetLastRequestDateIndex(s, identityId),
				FALLBACK_DATE_INDEX);
	}

	static DateIndex handleGetLastRequestDateIndex(PreparedStatement pstmt,
			Integer identityId) throws SQLException {

		DateIndex result = FALLBACK_DATE_INDEX;

		pstmt.setInt(1, identityId);

		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			String dateStr = rs.getString(1);
			int index = rs.getInt(2);
			if (dateStr != null && !rs.wasNull()) {
				LocalDate date = Utils.date(dateStr);
				result = new DateIndex(date, index);
			}
		}

		return result;
	}

	static boolean updateRequestHistory(JDBCWrapper jdbcWrapper,
			Integer identityId, RequestType type,
			LocalDate date, int index) {

		return jdbcWrapper.executeTransaction(
				w -> handleUpdateRequestHistory(w, identityId, type, date, index),
				false);
	}

	static boolean handleUpdateRequestHistory(JDBCWrapper jdbcWrapper,
			Integer identityId, RequestType type,
			LocalDate date, int index) throws SQLException {

		final String updateHistory = "UPDATE request_history "
			+ "SET " + getHistoryColumnName(type, "date") + "=?, "
			+ getHistoryColumnName(type, "index") + "=? "
			+ "WHERE identity_id=?";
		final String insertHistory = "INSERT INTO request_history "
			+ "(identity_id, " + getHistoryColumnName(type, "date") + ", "
			+ getHistoryColumnName(type, "index") + ") "
			+ "VALUES(?,?,?)";

		final String dateStr = Utils.format(date);

		int rowCount;
		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(updateHistory)) {
			pstmt.setString(1, dateStr);
			pstmt.setInt(2, index);
			pstmt.setInt(3, identityId);
			rowCount = pstmt.executeUpdate();
		}

		if (rowCount == 0) {
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertHistory)) {
				pstmt.setInt(1, identityId);
				pstmt.setString(2, dateStr);
				pstmt.setInt(3, index);
				pstmt.executeUpdate();
			}
		}

		return true;
	}

	static LocalDate getLastFailDate(JDBCWrapper jdbcWrapper,
			Integer identityId) {

		final String selectHistory = "SELECT last_fail_date "
			+ "FROM request_history "
			+ "WHERE identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectHistory,
				s -> handleGetLastFailDate(s, identityId),
				Constants.FALLBACK_DATE);
	}

	static LocalDate handleGetLastFailDate(PreparedStatement pstmt,
			Integer identityId) throws SQLException {

		LocalDate result = Constants.FALLBACK_DATE;

		pstmt.setInt(1, identityId);

		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			String dateStr = rs.getString(1);
			if (dateStr != null) {
				result = Utils.date(dateStr);
			}
		}

		return result;
	}

	static boolean updateLastFailDate(JDBCWrapper jdbcWrapper,
			Integer identityId, LocalDate date) {

		return jdbcWrapper.executeTransaction(
				w -> handleUpdateLastFailDate(w, identityId, date),
				false);
	}

	static boolean handleUpdateLastFailDate(JDBCWrapper jdbcWrapper,
			Integer identityId, LocalDate date) throws SQLException {

		final String updateHistory = "UPDATE request_history "
			+ "SET last_fail_date =? "
			+ "WHERE identity_id=?";
		final String insertHistory = "INSERT INTO request_history "
			+ "(identity_id, last_fail_date) "
			+ "VALUES(?,?)";

		final String dateStr = Utils.format(date);

		int rowCount;
		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(updateHistory)) {
			pstmt.setString(1, dateStr);
			pstmt.setInt(2, identityId);
			rowCount = pstmt.executeUpdate();
		}

		if (rowCount == 0) {
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertHistory)) {
				pstmt.setInt(1, identityId);
				pstmt.setString(2, dateStr);
				pstmt.executeUpdate();
			}
		}

		return true;
	}

	private static String getHistoryColumnName(RequestType type, String basename) {
		StringBuilder str = new StringBuilder("last_");

		switch (type) {
		case IDENTITY:
			str.append("identity");
			break;
		case TRUST_LIST:
			str.append("trustlist");
			break;
		case MESSAGE_LIST:
			str.append("messagelist");
			break;
		default:
			throw new AssertionError("invalid type: " + type.name());
		}

		str.append('_');
		str.append(basename);

		return str.toString();
	}

	private RequestHistoryImpl() {
	}
}
