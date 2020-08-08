package jfms.store;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.FmsManager;

public class FmsImport {
	public static final String TRUE_TEXT = "true";
	private static final Logger LOG = Logger.getLogger(FmsImport.class.getName());

	private final String fmsDbFile;
	private Connection fmsConn;
	private final Connection jfmsConn;

	public FmsImport(String fmsDbFile) {
		this.fmsDbFile = fmsDbFile;
		jfmsConn = FmsManager.getInstance().getStore().getConnection();
	}

	public boolean startImport() {
		boolean success = false;
		try {
			LOG.log(Level.INFO, "Starting FMS import from {0}", fmsDbFile);
			fmsConn = DriverManager.getConnection("jdbc:sqlite:" + fmsDbFile);
			jfmsConn.setAutoCommit(false);

			importIdentity();
			importRequestHistory();
			importBoard();
			importMessage();
			importBoardMessage();
			importMessageReplyTo();
			importAttachment();
			importPeerTrust();
			importLocalIdentity();
			importLocalMessage();
			importLocalTrust();

			success = true;
		} catch (SQLException e) {
			try {
				Utils.logSqlException("FMS import failed", e);
				jfmsConn.rollback();
			} catch (SQLException ex) {
				Utils.logSqlException("rollback failed", ex);
			}
		} finally {
			try {
				jfmsConn.setAutoCommit(true);
			} catch (SQLException ex) {
				Utils.logSqlException("failed to enable autocommit", ex);
			}
		}

		try {
			fmsConn.close();
		} catch (SQLException ex) {
			Utils.logSqlException("failed to close FMS DB", ex);
		}

		if (!success) {
			File failedFile = new File(Constants.DATABASE_FILE);
			failedFile.delete();
		}

		return success;
	}

	private void runImportStatements(String selectQuery, String insertQuery,
			FmsImportHandler handler) throws SQLException {

		try (PreparedStatement insertStmt = jfmsConn.prepareStatement(insertQuery)) {
			try (PreparedStatement pstmt = fmsConn.prepareStatement(selectQuery)) {
				ResultSet rs = pstmt.executeQuery();
				handler.run(rs, insertStmt);
			}

			insertStmt.executeBatch();
		}

		jfmsConn.commit();
	}

	private void importIdentity() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM identity");
		}

		final String selectQuery = "SELECT "
			+ "IdentityID, PublicKey, Name, Signature, FMSAvatar, SingleUse, "
			+ "PublishTrustList, PublishBoardList, FreeSiteEdition, "
			+ "DateAdded, AddedMethod "
			+ "FROM tblIdentity "
			+ "WHERE IsFMS=1";

		final String insertQuery = "INSERT INTO identity "
			+ "(identity_id, ssk, name, signature, avatar, single_use, "
			+ "publish_trustlist, publish_boardlist, freesite_edition, "
			+ "date_added, added_by) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportIdentity);
	}

	private static void handleImportIdentity(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		int defaultId = -1;
		while (rs.next()) {
			int id = rs.getInt(1);
			if (defaultId == -1) {
				defaultId = id;
			}

			insertStmt.setInt(1, id);                   // identity_id
			insertStmt.setString(2, rs.getString(2));   // ssk
			insertStmt.setString(3, rs.getString(3));   // name
			insertStmt.setString(4, rs.getString(4));   // signature
			insertStmt.setString(5, rs.getString(5));   // avatar

			// single_use
			insertStmt.setBoolean(6, TRUE_TEXT.equals(rs.getString(6)));

			// publish_trustlist
			insertStmt.setBoolean(7, TRUE_TEXT.equals(rs.getString(7)));

			// publish_boardlist
			insertStmt.setBoolean(8, TRUE_TEXT.equals(rs.getString(8)));

			insertStmt.setString(9, rs.getString(9));   // freesite_edition

			// date_added
			String dateTimeAdded = rs.getString(10);
			String dateAdded = null;
			if (dateTimeAdded != null && dateTimeAdded.length() >= 10) {
				dateAdded = dateTimeAdded.substring(0,10);
			}
			insertStmt.setString(10, dateAdded);

			// added_by
			String addedMethod = rs.getString(11);
			int addedBy = Constants.ADD_IMPORT;
			if ("Seed Identity".equals(addedMethod)) {
				addedBy = Constants.ADD_SEED_IDENTITY;
			}
			insertStmt.setInt(11, addedBy);

			insertStmt.addBatch();
		}

		// if (defaultId != -1) {
		//	Config.getInstance().setStringValue(
		// 			Config.DEFAULT_ID, Integer.toString(defaultId));
		//}
	}

	private void importRequestHistory() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM request_history");
		}

		final String selectQuery = "SELECT IdentityID, LastSeen "
			+ "FROM tblIdentity "
			+ "WHERE IsFMS=1";
		final String insertQuery = "INSERT INTO request_history "
			+ "(identity_id, last_identity_date, last_identity_index, "
			+ "last_fail_date) "
			+ "VALUES (?,?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportRequestHistory);
	}

	private static void handleImportRequestHistory(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		// set last fail date to yesterday
		// otherwise all identities will be marked as active and initial
		// identity download would take days/weeks
		String lastFailDate = Utils.format(LocalDate.now().minusDays(1));

		while (rs.next()) {
			// identity_id
			insertStmt.setInt(1, rs.getInt(1));

			// last_identity_date
			String lastSeenDate = rs.getString(2);
			String lastIdentityDate = null;
			if (lastSeenDate != null && lastSeenDate.length() >= 10) {
				lastIdentityDate = lastSeenDate.substring(0,10);
			}
			insertStmt.setString(2, lastIdentityDate);

			// last_identity_index
			insertStmt.setInt(3, 0);

			// last_fail_date
			insertStmt.setString(4, lastFailDate);

			insertStmt.addBatch();
		}
	}

	private void importBoard() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM board");
		}

		final String selectQuery = "SELECT BoardID, BoardName, Forum "
			+ "FROM tblBoard";
		final String insertQuery = "INSERT INTO board "
			+ "(board_id, name, subscribed) "
			+ "VALUES (?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportBoard);
	}

	private static void handleImportBoard(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		while (rs.next()) {
			insertStmt.setInt(1, rs.getInt(1));         // board_id
			insertStmt.setString(2, rs.getString(2));   // name

			// signature
			insertStmt.setBoolean(3, TRUE_TEXT.equals(rs.getString(3)));

			insertStmt.addBatch();
		}
	}

	private void importMessage() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM message");
		}

		final String selectQuery = "SELECT "
			+ "MessageID, IdentityID, MessageDate, MessageTime, Subject, "
			+ "MessageUUID, ReplyBoardID, InsertDate, MessageIndex, "
			+ "Body, Read "
			+ "FROM tblMessage";
		final String insertQuery = "INSERT INTO message "
			+ "(message_id, identity_id, date, time, subject,"
			+ "message_uuid, reply_board_id, insert_date, insert_index, "
			+ "body, read) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportMessage);
	}

	private static void handleImportMessage(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		while (rs.next()) {
			insertStmt.setInt(1, rs.getInt(1));         // message_id
			insertStmt.setInt(2, rs.getInt(2));         // identity_id
			insertStmt.setString(3, rs.getString(3));   // date
			insertStmt.setString(4, rs.getString(4));   // time
			insertStmt.setString(5, rs.getString(5));   // subject
			insertStmt.setString(6, rs.getString(6));   // message_uuid
			insertStmt.setInt(7, rs.getInt(7));         // reply_board_id
			insertStmt.setString(8, rs.getString(8));   // insert_date
			insertStmt.setInt(9, rs.getInt(9));         // insert_index
			insertStmt.setString(10, rs.getString(10)); // body
			insertStmt.setBoolean(11, rs.getInt(11) == 1); // read

			insertStmt.addBatch();
		}
	}

	private void importBoardMessage() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM board_message");
		}

		final String selectQuery = "SELECT BoardID, MessageID "
			+ "FROM tblMessageBoard";
		final String insertQuery = "INSERT INTO board_message "
			+ "(board_id, message_id) "
			+ "VALUES (?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportBoardMessage);
	}

	private static void handleImportBoardMessage(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		while (rs.next()) {
			insertStmt.setInt(1, rs.getInt(1)); // board_id
			insertStmt.setInt(2, rs.getInt(2)); // message_id

			insertStmt.addBatch();
		}
	}

	private void importMessageReplyTo() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM message_reply_to");
		}

		final String selectQuery = "SELECT MessageID, "
			+ "ReplyOrder, ReplyToMessageUUID "
			+ "FROM tblMessageReplyTo";
		final String insertQuery = "INSERT INTO message_reply_to "
			+ "(message_id, reply_order, message_uuid) "
			+ "VALUES (?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportMessageReplyTo);
	}

	private static void handleImportMessageReplyTo(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		while (rs.next()) {
			insertStmt.setInt(1, rs.getInt(1));       // message_id
			insertStmt.setInt(2, rs.getInt(2));       // reply_order
			insertStmt.setString(3, rs.getString(3)); // message_uuid

			insertStmt.addBatch();
		}
	}

	private void importAttachment() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM attachment");
		}

		final String selectQuery = "SELECT MessageID, Key, Size "
			+ "FROM tblMessageFileAttachment";
		final String insertQuery = "INSERT INTO attachment "
			+ "(message_id, uri, size) "
			+ "VALUES (?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportAttachment);
	}

	private static void handleImportAttachment(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		while (rs.next()) {
			insertStmt.setInt(1, rs.getInt(1));       // message_id
			insertStmt.setString(2, rs.getString(2)); // uri
			insertStmt.setInt(3, rs.getInt(3));       // size

			insertStmt.addBatch();
		}
	}

	private void importPeerTrust() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM peer_trust");
		}

		final String selectQuery = "SELECT IdentityID, TargetIdentityID, "
			+ "TrustListTrust, MessageTrust, "
			+ "TrustListTrustComment, MessageTrustComment "
			+ "FROM tblPeerTrust";
		final String insertQuery = "INSERT INTO peer_trust "
			+ "(identity_id, target_identity_id, "
			+ "trustlist_trust, message_trust, "
			+ "trustlist_trust_comment, message_trust_comment) "
			+ "VALUES (?,?,?,?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportTrust);
	}

	private static void handleImportTrust(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		while (rs.next()) {
			int identityId = rs.getInt(1);
			int targetIdentityId = rs.getInt(2);

			int trustListTrust = rs.getInt(3);
			if (rs.wasNull()) {
				trustListTrust = -1;
			}

			int messageTrust = rs.getInt(4);
			if (rs.wasNull()) {
				messageTrust = -1;
			}

			String msgTrustComment = rs.getString(5);
			if (msgTrustComment != null && msgTrustComment.isEmpty()) {
				msgTrustComment = null;
			}

			String tlTrustComment = rs.getString(6);
			if (tlTrustComment != null && tlTrustComment.isEmpty()) {
				tlTrustComment = null;
			}

			if (trustListTrust < 0 && messageTrust < 0 &&
					tlTrustComment == null && msgTrustComment == null) {

				continue;
			}

			insertStmt.setInt(1, identityId);
			insertStmt.setInt(2, targetIdentityId);

			if (trustListTrust >= 0) {
				insertStmt.setInt(3, trustListTrust);
			} else {
				insertStmt.setNull(3, Types.INTEGER);
			}

			if (messageTrust >= 0) {
				insertStmt.setInt(4, messageTrust);
			} else {
				insertStmt.setNull(4, Types.INTEGER);
			}

			insertStmt.setString(5, rs.getString(5));
			insertStmt.setString(6, rs.getString(6));

			insertStmt.addBatch();
		}
	}

	private void importLocalIdentity() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM local_identity");
		}

		final String selectQuery = "SELECT "
			+ "LocalIdentityID, PublicKey, PrivateKey, Name, Signature, "
			+ "FMSAvatar, SingleUse, PublishTrustList, PublishBoardList, "
			+ "Active, DateCreated "
			+ "FROM tblLocalIdentity";
		final String insertQuery = "INSERT INTO local_identity "
			+ "(local_identity_id, ssk, private_ssk, name, signature, "
			+ "avatar, single_use, publish_trustlist, publish_boardlist, "
			+ "active, creation_date) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportLocalIdentity);
	}

	private static void handleImportLocalIdentity(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		while (rs.next()) {
			insertStmt.setInt(1, rs.getInt(1));       // local_identity_id
			insertStmt.setString(2, rs.getString(2)); // ssk
			insertStmt.setString(3, rs.getString(3)); // private_ssk
			insertStmt.setString(4, rs.getString(4)); // name
			insertStmt.setString(5, rs.getString(5)); // signature
			insertStmt.setString(6, rs.getString(6)); // avatar

			// single_use
			insertStmt.setBoolean(7, TRUE_TEXT.equals(rs.getString(7)));

			// publish_trustlist
			insertStmt.setBoolean(8, TRUE_TEXT.equals(rs.getString(8)));

			// publish_boardlist
			insertStmt.setBoolean(9, TRUE_TEXT.equals(rs.getString(9)));

			// active
			insertStmt.setBoolean(10, TRUE_TEXT.equals(rs.getString(10)));

			// creation_date
			String creationDateTime = rs.getString(11);
			String creationDate = null;
			if (creationDateTime != null && creationDateTime.length() >= 10) {
				creationDate = creationDateTime.substring(0,10);
			}
			insertStmt.setString(11, creationDate);

			insertStmt.addBatch();
		}
	}

	private void importLocalMessage() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM local_message");
		}

		final String selectQuery = "SELECT "
			+ "LocalIdentityID, Day, InsertIndex, "
			+ "MessageXML, Inserted "
			+ "FROM tblMessageInserts";
		final String insertQuery = "INSERT INTO local_message "
			+ "(local_identity_id, insert_date, insert_index, "
			+ "message_xml, inserted) "
			+ "VALUES (?,?,?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportLocalMessage);
	}

	private static void handleImportLocalMessage(ResultSet rs,
			PreparedStatement insertStmt) throws SQLException {

		while (rs.next()) {
			insertStmt.setInt(1, rs.getInt(1));       // local_identity_id
			insertStmt.setString(2, rs.getString(2)); // insert_date
			insertStmt.setInt(3, rs.getInt(3));       // insert_index
			insertStmt.setString(4, rs.getString(4)); // message_xml
			// inserted
			insertStmt.setBoolean(5, TRUE_TEXT.equals(rs.getString(5)));

			insertStmt.addBatch();
		}
	}

	private void importLocalTrust() throws SQLException {
		try (Statement stmt = jfmsConn.createStatement()) {
			stmt.execute("DELETE FROM local_trust");
		}

		final String selectQuery = "SELECT LocalIdentityID, IdentityID, "
			+ "LocalTrustListTrust, LocalMessageTrust, "
			+ "TrustListTrustComment, MessageTrustComment "
			+ "FROM tblIdentityTrust";
		final String insertQuery = "INSERT INTO local_trust "
			+ "(local_identity_id, identity_id, "
			+ "trustlist_trust, message_trust, "
			+ "trustlist_trust_comment, message_trust_comment) "
			+ "VALUES (?,?,?,?,?,?)";

		runImportStatements(selectQuery, insertQuery,
				FmsImport::handleImportTrust);
	}
}
