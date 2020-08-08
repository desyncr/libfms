package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.fms.MessageReference;

public class LocalMessageImpl {
	private static final Logger LOG = Logger.getLogger(LocalMessageImpl.class.getName());

	static String getLocalMessage(JDBCWrapper jdbcWrapper,
			int localIdentityId, LocalDate insertDate, int insertIndex) {

		final String selectMessage = "SELECT message_xml "
			+ "FROM local_message "
			+ "WHERE local_identity_id=? AND insert_date=? AND insert_index=?";

		return jdbcWrapper.executePreparedStatement(selectMessage,
				s -> handleGetLocalMessage(s, localIdentityId, insertDate, insertIndex),
			null);
	}

	static String handleGetLocalMessage(PreparedStatement pstmt,
			int localIdentityId,LocalDate insertDate, int insertIndex)
		throws SQLException {

		pstmt.setInt(1, localIdentityId);
		pstmt.setString(2, Utils.format(insertDate));
		pstmt.setInt(3, insertIndex);

		ResultSet rs = pstmt.executeQuery();
		String messageXml = null;
		if (rs.next()) {
			messageXml = rs.getString(1);
		}

		if (messageXml == null) {
			LOG.log(Level.WARNING, "local message not found");
		}

		return messageXml;
	}

	static int saveLocalMessage(JDBCWrapper jdbcWrapper,
			int localIdentityId, String messageXml, LocalDate date,
			MessageReference draftRef, InsertStatus insertStatus) {

		return jdbcWrapper.executeTransaction(
			w -> handleSaveLocalMessage(w, localIdentityId, messageXml,
				date, draftRef, insertStatus),
			-1);
	}

	static int handleSaveLocalMessage(JDBCWrapper jdbcWrapper,
			int localIdentityId, String messageXml, LocalDate date,
			MessageReference draftRef, InsertStatus insertStatus)
		throws SQLException {

		final String selectLocalMessage = "SELECT MAX(insert_index) "
			+ "FROM local_message "
			+ "WHERE local_identity_id=? AND insert_date=?";
		final String insertLocalMessage = "INSERT INTO local_message "
			+ "(local_identity_id, insert_date, insert_index, "
			+ "message_xml, inserted) "
			+ "VALUES(?,?,?,?,?)";
		final String updateLocalMessage = "UPDATE local_message "
			+ "SET local_identity_id=?, insert_date=?, insert_index=?, "
			+ "message_xml=?, inserted=? "
			+ "WHERE local_identity_id=? AND insert_date=? AND insert_index=?";

		final int insertValue;
		switch (insertStatus) {
		case NOT_INSERTED:
			insertValue = 0;
			break;
		case DRAFT:
			insertValue = 2;
			break;
		default:
			LOG.log(Level.WARNING, "unexpected message status {0}", insertStatus);
			return -1;
		}

		final String dateStr = Utils.format(date);

		int maxIndex = -1;
		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(selectLocalMessage)) {
			pstmt.setInt(1, localIdentityId);
			pstmt.setString(2, dateStr);

			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				maxIndex = rs.getInt(1);
				if (rs.wasNull()) {
					maxIndex = -1;
				}
			}
		}

		// When existing messages are saved as draft or moved into outbox,
		// the old date/index slot is normally reused.
		// If the date changed since last save, we allocate a new date/index
		// slot with the new date

		final int newIndex;
		if (draftRef != null) {
			if (date.equals(draftRef.getDate()) &&
						localIdentityId == draftRef.getIdentityId()) {
				newIndex = draftRef.getIndex();
			} else {
				newIndex = maxIndex + 1;
			}
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(updateLocalMessage)) {
				final String draftDateStr = Utils.format(draftRef.getDate());

				pstmt.setInt(1, localIdentityId);
				pstmt.setString(2, dateStr);
				pstmt.setInt(3, newIndex);
				pstmt.setString(4, messageXml);
				pstmt.setInt(5, insertValue);
				pstmt.setInt(6, draftRef.getIdentityId());
				pstmt.setString(7, draftDateStr);
				pstmt.setInt(8, draftRef.getIndex());

				pstmt.executeUpdate();
			}
		} else {
			newIndex = maxIndex + 1;
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertLocalMessage)) {
				pstmt.setInt(1, localIdentityId);
				pstmt.setString(2, dateStr);
				pstmt.setInt(3, newIndex);
				pstmt.setString(4, messageXml);
				pstmt.setInt(5, insertValue);

				pstmt.executeUpdate();
			}
		}

		return newIndex;
	}

	static boolean setLocalMessageInserted(JDBCWrapper jdbcWrapper,
			int localIdentityId, LocalDate date, int index,
			boolean inserted) {

		final String updateLocalMessage = "UPDATE local_message " +
			"SET inserted=? " +
			"WHERE local_identity_id=? AND insert_date=? AND insert_index=?";

		return jdbcWrapper.executePreparedStatement(updateLocalMessage,
			(PreparedStatement pstmt) -> {
				pstmt.setBoolean(1, inserted);
				pstmt.setInt(2, localIdentityId);
				pstmt.setString(3, Utils.format(date));
				pstmt.setInt(4, index);
				pstmt.executeUpdate();
				return true;
			},
			false);
	}

	static boolean deleteLocalMessage(JDBCWrapper jdbcWrapper,
			int localIdentityId, LocalDate date, int index) {

		final String query = "DELETE FROM local_message "
			+ "WHERE local_identity_id=? AND insert_date=? AND insert_index=? "
			+ "AND inserted!=1";

		return jdbcWrapper.executePreparedStatement(query,
			(PreparedStatement pstmt) -> {
				pstmt.setInt(1, localIdentityId);
				pstmt.setString(2, Utils.format(date));
				pstmt.setInt(3, index);
				pstmt.executeUpdate();
				return true;
			},
			false);
	}

	static List<MessageReference> getLocalMessageList(JDBCWrapper jdbcWrapper,
			int localIdentityId, InsertStatus inserted,
			LocalDate fromDate, int limit) {

		StringBuilder str = new StringBuilder();

		str.append("SELECT insert_date, insert_index ");
		str.append("FROM local_message ");
		str.append("WHERE local_identity_id=? ");

		if (fromDate != null) {
			str.append("AND insert_date>=? ");
		}

		switch (inserted) {
		case IGNORE:
			break;
		case INSERTED:
			str.append("AND inserted=1 ");
			break;
		case NOT_INSERTED:
			str.append("AND inserted=0 ");
			break;
		case DRAFT:
			str.append("AND inserted=2 ");
			break;
		}

		str.append("ORDER BY insert_date DESC, insert_index DESC ");
		if (limit >= 0) {
			str.append("LIMIT ?");
		}

		return jdbcWrapper.executePreparedStatement(str.toString(),
				s -> handleGetLocalMessageList(s, localIdentityId, fromDate, limit),
				Collections.emptyList());
	}

	static List<MessageReference> handleGetLocalMessageList(
			PreparedStatement pstmt, int localIdentityId,
			LocalDate fromDate, int limit)
		throws SQLException {

		List<MessageReference> messages = new ArrayList<>();

		int idx = 1;
		pstmt.setInt(idx++, localIdentityId);
		if (fromDate != null) {
			pstmt.setString(idx++, Utils.format(fromDate));
		}
		if (limit >= 0) {
			pstmt.setInt(idx++, limit);
		}

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			MessageReference msgRef = new MessageReference();
			msgRef.setDate(Utils.date(rs.getString(1)));
			msgRef.setIndex(rs.getInt(2));

			messages.add(msgRef);
		}

		return messages;
	}

	private LocalMessageImpl() {
	}
}
