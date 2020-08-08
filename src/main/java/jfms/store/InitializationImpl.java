package jfms.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;

public class InitializationImpl {
	private static final Logger LOG = Logger.getLogger(InitializationImpl.class.getName());

	static void createTables(Connection connection) throws SQLException {
		LOG.log(Level.INFO, "Initializing SQLite DB");

		final String createJfmsInfoTable = "CREATE TABLE IF NOT EXISTS jfms_info("
			+ "key TEXT NOT NULL, "
			+ "value TEXT, "
			+ "PRIMARY KEY(key)"
			+ ")";

		final String createIdentityTable = "CREATE TABLE IF NOT EXISTS identity("
			+ "identity_id INTEGER NOT NULL, "
			+ "ssk VARCHAR(100) UNIQUE NOT NULL, "
			+ "name VARCHAR(40), "
			+ "signature TEXT, "
			+ "avatar TEXT, "
			+ "single_use BOOLEAN DEFAULT 0, "
			+ "publish_trustlist BOOLEAN DEFAULT 0, "
			+ "publish_boardlist BOOLEAN DEFAULT 0, "
			+ "freesite_edition INTEGER, "
			+ "date_added DATE, "
			+ "added_by INTEGER, "
			+ "PRIMARY KEY(identity_id)"
			+ ")";

		final String createLocalIdentityTable = "CREATE TABLE IF NOT EXISTS local_identity("
			+ "local_identity_id INTEGER NOT NULL, "
			+ "ssk VARCHAR(100) UNIQUE NOT NULL, "
			+ "private_ssk VARCHAR(101) UNIQUE NOT NULL, "
			+ "name VARCHAR(40), "
			+ "signature TEXT, "
			+ "avatar TEXT, "
			+ "single_use BOOLEAN DEFAULT 0, "
			+ "publish_trustlist BOOLEAN DEFAULT 0, "
			+ "publish_boardlist BOOLEAN DEFAULT 0, "
			+ "active BOOLEAN DEFAULT 0, "
			+ "creation_date DATE, "
			+ "PRIMARY KEY(local_identity_id)"
			+ ")";

		final String createLocalIdentityInsertTable = "CREATE TABLE IF NOT EXISTS local_identity_insert("
			+ "local_identity_id INTEGER NOT NULL, "
			+ "insert_date DATE, "
			+ "insert_index INTEGER, "
			+ "PRIMARY KEY(local_identity_id, insert_date, insert_index)"
			+")";

		final String createTrustListInsertTable = "CREATE TABLE IF NOT EXISTS trustlist_insert("
			+ "local_identity_id INTEGER NOT NULL, "
			+ "insert_date DATE, "
			+ "insert_index INTEGER, "
			+ "inserted BOOLEAN DEFAULT 0,"
			+ "PRIMARY KEY(local_identity_id, insert_date, insert_index)"
			+")";

		final String createMessageListInsertTable = "CREATE TABLE IF NOT EXISTS messagelist_insert("
			+ "local_identity_id INTEGER NOT NULL, "
			+ "insert_date DATE, "
			+ "insert_index INTEGER, "
			+ "inserted BOOLEAN DEFAULT 0,"
			+ "PRIMARY KEY(local_identity_id, insert_date, insert_index)"
			+")";


		final String createLocalMessageTable = "CREATE TABLE IF NOT EXISTS local_message("
			+ "local_identity_id INTEGER NOT NULL, "
			+ "insert_date DATE, "
			+ "insert_index INTEGER, "
			+ "message_xml TEXT, "
			+ "inserted BOOLEAN DEFAULT 0, "
			+ "PRIMARY KEY(local_identity_id, insert_date, insert_index)"
			+ ")";

		final String createTrustTable = "CREATE TABLE IF NOT EXISTS peer_trust("
			+ "identity_id INTEGER NOT NULL, "
			+ "target_identity_id INTEGER, "
			+ "trustlist_trust INTEGER, "
			+ "message_trust INTEGER, "
			+ "trustlist_trust_comment VARCHAR(250), "
			+ "message_trust_comment VARCHAR(250), "
			+ "PRIMARY KEY(identity_id, target_identity_id)"
			+ ")";

		final String createMessageTable = "CREATE TABLE IF NOT EXISTS message("
			+ "message_id INTEGER NOT NULL, "
			+ "identity_id INTEGER, "
			+ "date DATE, "
			+ "time TIME, "
			+ "subject VARCHAR(250), "
			+ "message_uuid VARCHAR(80) UNIQUE, "
			+ "reply_board_id INTEGER, "
			+ "insert_date DATE, "
			+ "insert_index INTEGER, "
			+ "body TEXT, "
			+ "read BOOLEAN DEFAULT 0, "
			+ "flags INTEGER, "
			+ "PRIMARY KEY(message_id)"
			+ ")";

		final String createInReplyToTable = "CREATE TABLE IF NOT EXISTS message_reply_to("
			+ "message_id INTEGER NOT NULL, "
			+ "reply_order INTEGER NOT NULL, "
			+ "message_uuid VARCHAR(80), "
			+ "PRIMARY KEY(message_id, reply_order)"
			+ ")";

		final String createAttachmentTable = "CREATE TABLE IF NOT EXISTS attachment("
			+ "attachment_id INTEGER NOT NULL, "
			+ "message_id INTEGER NOT NULL, "
			+ "uri TEXT, "
			+ "size INTEGER, "
			+ "PRIMARY KEY(attachment_id)"
			+ ")";

		final String createBoardTable = "CREATE TABLE IF NOT EXISTS board("
			+ "board_id INTEGER NOT NULL, "
			+ "name VARCHAR(40) UNIQUE, "
			+ "subscribed BOOLEAN, "
			+ "PRIMARY KEY(board_id)"
			+ ")";

		final String createBoardMessageTable = "CREATE TABLE IF NOT EXISTS board_message("
			+ "board_id INTEGER NOT NULL, "
			+ "message_id INTEGER NOT NULL, "
			+ "PRIMARY KEY(board_id, message_id)"
			+ ")";

		final String createLocalTrustTable = "CREATE TABLE IF NOT EXISTS local_trust("
			+ "local_identity_id INTEGER NOT NULL, "
			+ "identity_id INTEGER NOT NULL, "
			+ "trustlist_trust INTEGER, "
			+ "message_trust INTEGER, "
			+ "trustlist_trust_comment VARCHAR(250), "
			+ "message_trust_comment VARCHAR(250), "
			+ "PRIMARY KEY(local_identity_id, identity_id)"
			+ ")";

		final String createIntroductionPuzzleTable =
			"CREATE TABLE IF NOT EXISTS introduction_puzzle ("
			+ "local_identity_id INTEGER NOT NULL, "
			+ "insert_date DATE, "
			+ "insert_index INTEGER, "
			+ "uuid VARCHAR(80), "
			+ "solution TEXT, "
			+ "solved BOOLEAN DEFAULT 0, "
			+ "PRIMARY KEY(local_identity_id, insert_date, insert_index)"
			+ ")";

		final String createIdentityIntroductionTable =
			"CREATE TABLE IF NOT EXISTS identity_introduction_insert ("
			+ "local_identity_id INTEGER NOT NULL, "
			+ "insert_date DATE, "
			+ "uuid VARCHAR(80), "
			+ "solution TEXT, "
			+ "inserted BOOLEAN DEFAULT 0, "
			+ "PRIMARY KEY(local_identity_id, insert_date, uuid)"
			+ ")";

		final String createRequestHistoryTable = "CREATE TABLE IF NOT EXISTS request_history("
			+ "identity_id INTEGER NOT NULL, "
			+ "last_identity_date DATE, "
			+ "last_identity_index INTEGER, "
			+ "last_fail_date DATE, "
			+ "last_trustlist_date DATE, "
			+ "last_trustlist_index INTEGER, "
			+ "last_messagelist_date DATE, "
			+ "last_messagelist_index INTEGER, "
			+ "PRIMARY KEY(identity_id)"
			+")";

		final String createAvatarTable = "CREATE TABLE IF NOT EXISTS avatar("
			+ "identity_id INTEGER NOT NULL, "
			+ "extension STRING, "
			+ "disabled BOOLEAN, "
			+ "tries INTEGER, "
			+ "last_fail_date DATE, "
			+ "PRIMARY KEY(identity_id)"
			+")";

		try {
			connection.setAutoCommit(false);

			try (Statement statement = connection.createStatement()) {

				statement.addBatch(createJfmsInfoTable);
				statement.addBatch(createIdentityTable);
				statement.addBatch(createLocalIdentityTable);
				statement.addBatch(createLocalIdentityInsertTable);
				statement.addBatch(createTrustListInsertTable);
				statement.addBatch(createMessageListInsertTable);
				statement.addBatch(createLocalMessageTable);
				statement.addBatch(createTrustTable);
				statement.addBatch(createMessageTable);
				statement.addBatch(createInReplyToTable);
				statement.addBatch(createAttachmentTable);
				statement.addBatch(createBoardTable);
				statement.addBatch(createBoardMessageTable);
				statement.addBatch(createLocalTrustTable);
				statement.addBatch(createIntroductionPuzzleTable);
				statement.addBatch(createIdentityIntroductionTable);
				statement.addBatch(createRequestHistoryTable);
				statement.addBatch(createAvatarTable);

				statement.executeBatch();
			}
			connection.commit();
		} catch (SQLException e) {
			try {
				Utils.logSqlException("SQLite DB initialization failed", e);
				connection.rollback();
			} catch (SQLException ex) {
				Utils.logSqlException("rollback failed", ex);
			}

			throw e;
		} finally {
			connection.setAutoCommit(true);
		}
	}

	static boolean saveSeedIdentities(JDBCWrapper jdbcWrapper,
			List<String> seedIdentities) {

		return jdbcWrapper.executeTransaction(
				w -> handleSaveSeedIdentities(w, seedIdentities),
				false);
	}

	static boolean handleSaveSeedIdentities(JDBCWrapper jdbcWrapper,
			List<String> seedIdentities) throws SQLException {

		final String identityCountQuery = "SELECT COUNT(*) FROM identity "
				+ "WHERE ssk=?";
		final String insertSeedIdentity =
			"INSERT INTO identity (ssk, date_added, added_by) VALUES(?,?,?)";
		final LocalDate now = LocalDate.now(ZoneOffset.UTC);

		LOG.log(Level.INFO, "Populating database with seed identities");

		for (String ssk : seedIdentities) {
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(identityCountQuery)) {

				pstmt.setString(1, ssk);

				ResultSet rs = pstmt.executeQuery();
				if (rs.next()) {
					int identityCount = rs.getInt(1);
					if (identityCount > 0) {
						continue;
					}
				}
			}

			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertSeedIdentity)) {

				pstmt.setString(1, ssk);
				pstmt.setString(2, Utils.format(now));
				pstmt.setInt(3, Constants.ADD_SEED_IDENTITY);
				pstmt.executeUpdate();
			}
		}

		return true;
	}

	static boolean saveSeedBoards(JDBCWrapper jdbcWrapper) {
		return jdbcWrapper.executeTransaction(InitializationImpl::handleSaveSeedBoards,
				false);
	}

	static boolean handleSaveSeedBoards(JDBCWrapper jdbcWrapper)
		throws SQLException {

		final String boardCountQuery = "SELECT COUNT(*) FROM board";
		final String insertSeedBoard = "INSERT INTO board (name, subscribed) "
			+ "VALUES(?,1)";

		try (Statement stmt = jdbcWrapper.createStatement(boardCountQuery)) {
			ResultSet resultSet = stmt.executeQuery(boardCountQuery);
			if (resultSet.next()) {
				int boardCount = resultSet.getInt(1);
				if (boardCount > 0) {
					return true;
				}
			}
		}

		LOG.log(Level.INFO, "Populating database with seed boards");
		final String[] seedBoardNames = {
                "jfms/fms", "freenet", "jfms", "public", "test"
		};

		try (PreparedStatement statement = jdbcWrapper.prepareStatement(insertSeedBoard)) {
			for (String boardName : seedBoardNames) {
				statement.setString(1, boardName);
				statement.addBatch();
			}

			statement.executeBatch();
		}

		return true;
	}

	private InitializationImpl() {
	}
}
