package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import jfms.fms.RequestType;

public class InsertTablesImpl {
	static int getInsertIndex(JDBCWrapper jdbcWrapper,
			RequestType type, int localIdentityId,
			LocalDate date, InsertStatus inserted) {

		StringBuilder str = new StringBuilder();
		str.append("SELECT MAX(insert_index) ");
		str.append("FROM ");
		final String table = getInsertTableName(type);
		str.append(table);
		str.append(" WHERE local_identity_id=? AND insert_date=?");

		switch (inserted) {
		case IGNORE:
			break;
		case INSERTED:
			str.append(" AND inserted = 1 ");
			break;
		case NOT_INSERTED:
			str.append(" AND inserted = 0 ");
			break;
		}

		return jdbcWrapper.executePreparedStatement(str.toString(),
				s -> handleGetInsertIndex(s, localIdentityId, date),
				-1);
	}

	static int handleGetInsertIndex(PreparedStatement pstmt,
			int localIdentityId, LocalDate date) throws SQLException {

		int maxIndex = -1;
		pstmt.setInt(1, localIdentityId);
		pstmt.setString(2, Utils.format(date));

		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			maxIndex = rs.getInt(1);
			if (rs.wasNull()) {
				maxIndex = -1;
			}
		}

		return maxIndex;
	}

	static boolean incrementInsertIndex(JDBCWrapper jdbcWrapper,
			RequestType type, int localIdentityId,
			LocalDate date, int index) {

		final String table = getInsertTableName(type);
		final String updateQuery = "UPDATE " + table
			+ " SET insert_index=("
				+ "SELECT MAX(insert_index)+1 "
				+ "FROM " + table
				+ " WHERE local_identity_id=? AND insert_date=?) "
			+ "WHERE local_identity_id=? AND insert_date=? AND insert_index=?";

		return jdbcWrapper.executePreparedStatement(updateQuery,
				s -> handleIncrementInsertIndex(s, localIdentityId, date, index),
				false);
	}

	static boolean handleIncrementInsertIndex(PreparedStatement pstmt,
			int localIdentityId, LocalDate date, int index)
		throws SQLException {

		final String dateStr = Utils.format(date);

		pstmt.setInt(1, localIdentityId);
		pstmt.setString(2, dateStr);
		pstmt.setInt(3, localIdentityId);
		pstmt.setString(4, dateStr);
		pstmt.setInt(5, index);
		pstmt.executeUpdate();

		return true;
	}

	static boolean updateInsert(JDBCWrapper jdbcWrapper,
			RequestType type, Integer localIdentityId,
			LocalDate date, int index, InsertStatus inserted) {

		return jdbcWrapper.executeTransaction(
			w -> handleUpdateInsert(w, type, localIdentityId, date, index, inserted),
			false);
	}

	static boolean handleUpdateInsert(JDBCWrapper jdbcWrapper,
		RequestType type, Integer localIdentityId, LocalDate date, int index,
		InsertStatus inserted) throws SQLException {

		final String table = getInsertTableName(type);

		if ((type == RequestType.IDENTITY && inserted !=
					InsertStatus.IGNORE) || type == RequestType.MESSAGE) {
			throw new IllegalArgumentException("updateInsert: invalid parameter combination");
		}

		StringBuilder str = new StringBuilder();
		str.append("UPDATE ");
		str.append(table);
		str.append(" SET insert_date=?, insert_index=?");
		if (inserted != InsertStatus.IGNORE) {
			str.append(", inserted=?");
		}
		str.append(" WHERE local_identity_id=?");
		final String updateInsert = str.toString();

		str.setLength(0);
		str.append("INSERT INTO ");
		str.append(table);
		str.append(" (local_identity_id, insert_date, insert_index");
		if (inserted != InsertStatus.IGNORE) {
			str.append(", inserted");
		}
		str.append(") VALUES(?,?,?");
		if (inserted != InsertStatus.IGNORE) {
			str.append(",?");
		}
		str.append(')');
		final String insertInsert = str.toString();

		final String dateStr = Utils.format(date);

		int rowCount;
		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(updateInsert)) {
			int idx = 1;
			pstmt.setString(idx++, dateStr);
			pstmt.setInt(idx++, index);
			switch (inserted) {
			case IGNORE:
				break;
			case INSERTED:
				pstmt.setBoolean(idx++, true);
				break;
			case NOT_INSERTED:
				pstmt.setBoolean(idx++, false);
				break;
			}
			pstmt.setInt(idx++, localIdentityId);
			rowCount = pstmt.executeUpdate();
		}

		if (rowCount == 0) {
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertInsert)) {
				pstmt.setInt(1, localIdentityId);
				pstmt.setString(2, dateStr);
				pstmt.setInt(3, index);
				switch (inserted) {
				case IGNORE:
					break;
				case INSERTED:
					pstmt.setBoolean(4, true);
					break;
				case NOT_INSERTED:
					pstmt.setBoolean(4, false);
					break;
				}
				pstmt.executeUpdate();
			}
		}

		return true;
	}

	private static String getInsertTableName(RequestType type) {
		String str;

		switch (type) {
		case IDENTITY:
			str = "local_identity_insert";
			break;
		case TRUST_LIST:
			str = "trustlist_insert";
			break;
		case MESSAGE:
			str = "local_message";
			break;
		case MESSAGE_LIST:
			str = "messagelist_insert";
			break;
		default:
			throw new AssertionError("invalid type: " + type.name());
		}

		return str;
	}

	private InsertTablesImpl() {
	}
}
