package jfms.store;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.AddedInfo;
import jfms.fms.Attachment;
import jfms.fms.Avatar;
import jfms.fms.DateIndex;
import jfms.fms.Identity;
import jfms.fms.IdentityIntroduction;
import jfms.fms.IntroductionPuzzle;
import jfms.fms.LocalIdentity;
import jfms.fms.Message;
import jfms.fms.MessageReference;
import jfms.fms.RequestType;
import jfms.fms.Trust;
import jfms.util.Version;

public final class Store {
	private static final Logger LOG = Logger.getLogger(Store.class.getName());
	private static final String DB_VERSION_KEY = "db_version";
	private static final String DB_VERSION = "0.3";

	private final Connection connection;
	private final JDBCWrapper jdbcWrapper;
	private final String info;

	public static boolean databaseExists(String dbName) {
		boolean exists = Files.exists(Paths.get(dbName));
		if (!exists) {
			LOG.log(Level.INFO, "database file {0} not found",
				Constants.DATABASE_FILE);
		}

		return exists;
	}

	public Store(String dbURL) throws SQLException {
		Properties properties = new Properties();
		if (Constants.DATABASE_DRIVER != null) {
			try {
				Class.forName(Constants.DATABASE_DRIVER);
			} catch (ClassNotFoundException e) {
				LOG.log(Level.WARNING, "failed to load driver " + Constants.DATABASE_DRIVER, e);
			}
		}
		if (Constants.DATABASE_USER != null) {
			properties.put("user", Constants.DATABASE_USER);
		}
		if (Constants.DATABASE_PASSWORD != null) {
			properties.put("password", Constants.DATABASE_PASSWORD);
		}

		connection = DriverManager.getConnection(dbURL, properties);
		jdbcWrapper = new JDBCWrapper(connection);

		DatabaseMetaData metadata = connection.getMetaData();

		LOG.log(Level.INFO, "Using driver ''{0}'' version ''{1}''", new Object[]{
				metadata.getDriverName(), metadata.getDriverVersion()});
		LOG.log(Level.INFO, "Database is ''{0}'' version ''{1}''", new Object[]{
				metadata.getDatabaseProductName(),
				metadata.getDatabaseProductVersion()});

		info = metadata.getDriverName()
			+ " (" + metadata.getDriverVersion() + ')';

		InitializationImpl.createTables(connection);

		checkVersion();
	}

	public void initialize(List<String> seedIdentities) throws SQLException {
		if (seedIdentities != null) {
			InitializationImpl.saveSeedIdentities(jdbcWrapper, seedIdentities);
		}
		InitializationImpl.saveSeedBoards(jdbcWrapper);
	}

	public Connection getConnection() {
		return connection;
	}

	public void close() throws SQLException {
		connection.close();
	}

	public String getInfo() {
		return info;
	}


	//----- IDENTITY -----//

	public synchronized Map<Integer, Identity> getIdentities() {
		return IdentityImpl.getIdentities(jdbcWrapper);
	}

	public synchronized void updateIdentity(int identityId, Identity identity) {
		IdentityImpl.updateIdentity(jdbcWrapper, identityId, identity);
	}

	public synchronized List<Integer> getActiveIdentities(
			LocalDate activeSince, LocalDate singleUseAddedSince) {

		return IdentityImpl.getActiveIdentities(jdbcWrapper,
				activeSince, singleUseAddedSince);
	}

	public synchronized List<Integer> getInactiveIdentities(
			LocalDate inactiveSince, LocalDate notFailedSince,
			LocalDate singleUseAddedSince) {

		return IdentityImpl.getInactiveIdentities(jdbcWrapper,
				inactiveSince, notFailedSince, singleUseAddedSince);
	}

	public synchronized List<String> getSeedIdentitySsks() {
		return IdentityImpl.getSeedIdentitySsks(jdbcWrapper);
	}

	public synchronized Set<String> getRecentSsks(LocalDate fromDate) {
		return IdentityImpl.getRecentSsks(jdbcWrapper, fromDate);
	}

	public synchronized AddedInfo getAddedInfo(int identityId) {
		return IdentityImpl.getAddedInfo(jdbcWrapper, identityId);
	}

	public synchronized int saveIdentity(int trusterId, String ssk) {
		return saveIdentity(trusterId, ssk, LocalDate.now());
	}

	public synchronized int saveIdentity(int trusterId, String ssk,
			LocalDate date) {
		return IdentityImpl.saveIdentity(jdbcWrapper, trusterId, ssk, date);
	}

	//----- PEER TRUST -----//

	public synchronized List<Trust> getTrustList(int identityId) {
		return PeerTrustImpl.getTrustList(jdbcWrapper, identityId);
	}

	public synchronized List<Trust> getNumericTrustList(int identityId) {
		return PeerTrustImpl.getNumericTrustList(jdbcWrapper, identityId);
	}

	public synchronized List<Trust> getNumericTrusteeList(
			int targetIdentityId) {

		return PeerTrustImpl.getNumericTrusteeList(jdbcWrapper,
				targetIdentityId);
	}

	public synchronized Map<Integer, String> saveTrustList(
			int trusterId, List<Trust> trustList) {

		return PeerTrustImpl.saveTrustList(jdbcWrapper, trusterId, trustList);
	}

	public synchronized Map<Integer, Map<Integer, Integer>> getPeerTrusts() {
		return PeerTrustImpl.getPeerTrusts(jdbcWrapper);
	}

	//----- MESSAGE -----//

	public synchronized Message getMessage(int messageId) {
		return MessageImpl.getMessage(jdbcWrapper, messageId);
	}

	public synchronized String getMessageBody(int messageId) {
		return MessageImpl.getMessageBody(jdbcWrapper, messageId);
	}

	public synchronized List<Attachment> getAttachments(int messageId) {
		return MessageImpl.getAttachments(jdbcWrapper, messageId);
	}

	public synchronized boolean messageExists(int identityId,
			LocalDate insertDate, int insertIndex) {

		return MessageImpl.messageExists(jdbcWrapper,
				identityId, insertDate, insertIndex);
	}

	public synchronized int saveMessage(Message message,
			Map<Integer, String> newBoards) {

		return MessageImpl.saveMessage(jdbcWrapper, message, newBoards);
	}

	public synchronized boolean removeMessage(int messageId) {
		return MessageImpl.removeMessage(jdbcWrapper, messageId);
	}

	public synchronized void setMessageRead(int messageId, boolean read) {
		MessageImpl.setMessageRead(jdbcWrapper, messageId, read);
	}

	public synchronized void setBoardMessagesRead(int boardId, boolean read) {
		MessageImpl.setBoardMessagesRead(jdbcWrapper, boardId, read);
	}

	public synchronized void setAllMessagesRead() {
		MessageImpl.setAllMessagesRead(jdbcWrapper);
	}

	public synchronized void setMessageFlags(int messageId, int flags) {
		MessageImpl.setMessageFlags(jdbcWrapper, messageId, flags);
	}

	public synchronized int getMessageCount(int identityId) {
		return MessageImpl.getMessageCount(jdbcWrapper, identityId);
	}


	public synchronized List<MessageReference> getExternalMessageList(int identityId, int limit) {

		return MessageImpl.getExternalMessageList(jdbcWrapper, identityId, limit);
	}


	//----- BOARD -----//

	public synchronized Map<String, Integer> getBoardNames() {
		return BoardImpl.getBoardNames(jdbcWrapper);
	}

	public synchronized Map<String, Integer> getBoardInfos() {
		return BoardImpl.getBoardInfos(jdbcWrapper);
	}

	public synchronized List<String> getSubscribedBoardNames() {
		return BoardImpl.getSubscribedBoardNames(jdbcWrapper);
	}

	public synchronized void setBoardSubscribed(int boardId,
			boolean subscribed) {
		BoardImpl.setBoardSubscribed(jdbcWrapper, boardId, subscribed);
	}

	public synchronized int getUnreadMessageCount(int boardId) {
		return BoardImpl.getUnreadMessageCount(jdbcWrapper, boardId);
	}

	public synchronized List<Message> getMessagesForBoard(String board) {
		return MessageSearchImpl.getMessagesForBoard(jdbcWrapper, board);
	}

	public synchronized List<Message> getRecentMessages(boolean subscribedOnly) {
		return MessageSearchImpl.getRecentMessages(jdbcWrapper, subscribedOnly);
	}

	public synchronized List<Message> getStarredMessages() {
		return MessageSearchImpl.getStarredMessages(jdbcWrapper);
	}

	public synchronized List<Message> findMessages(MessageSearchCriteria msc) {

		return MessageSearchImpl.findMessages(jdbcWrapper, msc);
	}

	public synchronized int saveBoard(String boardName, boolean subscribed) {
		return BoardImpl.saveBoard(jdbcWrapper, boardName, subscribed);
	}


	//----- REQUEST HISTORY -----//

	public synchronized DateIndex getLastRequestDateIndex(Integer identityId,
			RequestType type) {

		return RequestHistoryImpl.getLastRequestDateIndex(jdbcWrapper,
				identityId, type);
	}

	public synchronized void updateRequestHistory(Integer identityId,
			RequestType type, LocalDate date, int index) {

		RequestHistoryImpl.updateRequestHistory(jdbcWrapper,
				identityId, type, date, index);
	}

	public synchronized LocalDate getLastFailDate(Integer identityId) {
		return RequestHistoryImpl.getLastFailDate(jdbcWrapper, identityId);
	}

	public synchronized void updateLastFailDate(Integer identityId,
			LocalDate date) {

		RequestHistoryImpl.updateLastFailDate(jdbcWrapper, identityId, date);
	}


	//----- LOCAL IDENTITY -----//

	public synchronized Map<Integer, LocalIdentity> retrieveLocalIdentities() {
		return LocalIdentityImpl.retrieveLocalIdentities(jdbcWrapper);
	}

	public synchronized LocalIdentity retrieveLocalIdentity(
			int localIdentityId) {

		return LocalIdentityImpl.retrieveLocalIdentity(jdbcWrapper,
				localIdentityId);
	}

	public synchronized int saveLocalIdentity(LocalIdentity identity) {
		return LocalIdentityImpl.saveLocalIdentity(jdbcWrapper, identity);
	}

	public synchronized boolean updateLocalIdentity(
			LocalIdentity localIdentity) {

		return LocalIdentityImpl.updateLocalIdentity(jdbcWrapper,
				localIdentity);
	}

	public synchronized boolean setLocalIdentityActive(int localIdentityId,
			boolean active) {

		return LocalIdentityImpl.setLocalIdentityActive(jdbcWrapper,
				localIdentityId, active);
	}

	public synchronized void deleteLocalIdentity(int localIdentityId) {
		LocalIdentityImpl.deleteLocalIdentity(jdbcWrapper, localIdentityId);
	}


	//----- LOCAL TRUST -----//

	public synchronized Trust getLocalTrust(int localIdentityId,
			int identityId) {

		return LocalTrustImpl.getLocalTrust(jdbcWrapper,
				localIdentityId, identityId);
	}

	public synchronized Map<Integer,Integer> getLocalTrustListTrusts(
		int localIdentityId, int minTrust) {

		return LocalTrustImpl.getLocalTrustListTrusts(jdbcWrapper,
				localIdentityId, minTrust);
	}

	public synchronized Map<Integer,Integer> getLocalMessageTrusts(
		int localIdentityId) {

		return LocalTrustImpl.getLocalMessageTrusts(jdbcWrapper,
				localIdentityId);
	}

	public synchronized List<Trust> getLocalTrustList(int localIdentityId) {
		return LocalTrustImpl.getLocalTrustList(jdbcWrapper, localIdentityId);
	}

	public void saveSeedTrust(Integer localIdentityId) {
		LocalTrustImpl.saveSeedTrust(jdbcWrapper, localIdentityId);
	}

	/**
	 * Updates local trust.
	 * @param localIdentityId numeric ID of local identity
	 * @param identityId numeric ID of identity to change
	 * @param trust new trust values to apply.
	 *
	 * The identity of parameter trust is ignored. Instead the numeric
	 * identityId will be used.
	 */
	public synchronized void updateLocalTrust(Integer localIdentityId,
			Integer identityId, Trust trust) {

		LocalTrustImpl.updateLocalTrust(jdbcWrapper,
				localIdentityId, identityId, trust);
	}


	//----- LOCAL MESSAGE -----//

	public synchronized String getLocalMessage(
			int localIdentityId, LocalDate insertDate, int insertIndex) {

		return LocalMessageImpl.getLocalMessage(jdbcWrapper,
				localIdentityId, insertDate, insertIndex);
	}

	public synchronized int saveLocalMessage(int localIdentityId,
			String messageXml, LocalDate date,
			MessageReference draftRef, InsertStatus insertStatus) {

		return LocalMessageImpl.saveLocalMessage(jdbcWrapper,
				localIdentityId, messageXml, date, draftRef, insertStatus);
	}

	public synchronized void setLocalMessageInserted(int localIdentityId,
		LocalDate date, int index, boolean inserted) {

		LocalMessageImpl.setLocalMessageInserted(jdbcWrapper,
				localIdentityId, date, index, inserted);
	}

	public synchronized void deleteLocalMessage(int localIdentityId,
			LocalDate date, int index) {

		LocalMessageImpl.deleteLocalMessage(jdbcWrapper,
				localIdentityId, date, index);
	}

	public synchronized List<MessageReference> getLocalMessageList(
			int localIdentityId, InsertStatus inserted,
			LocalDate fromDate, int limit) {

		// XXX do we need limit parameter?
		return LocalMessageImpl.getLocalMessageList(jdbcWrapper,
				localIdentityId, inserted, fromDate, limit);
	}


	//----- INTRODUCTION PUZZLE -----//

	public synchronized Map<DateIndex, IntroductionPuzzle> getUnsolvedPuzzles(
			int localIdentityId, LocalDate fromDate) {

		return IntroductionPuzzleImpl.getUnsolvedPuzzles(jdbcWrapper,
				localIdentityId, fromDate);
	}

	public synchronized int getUnsolvedPuzzleCount(
			int localIdentityId, LocalDate date) {

		return IntroductionPuzzleImpl.getUnsolvedPuzzleCount(jdbcWrapper,
				localIdentityId, date);
	}

	public synchronized int getNextIntroductionPuzzleIndex(
			int localIdentityId, LocalDate date) {

		return IntroductionPuzzleImpl.getNextIntroductionPuzzleIndex(
				jdbcWrapper, localIdentityId, date);
	}

	public synchronized void saveIntroductionPuzzle(int localIdentityId,
			LocalDate date, int index, IntroductionPuzzle puzzle) {

		IntroductionPuzzleImpl.saveIntroductionPuzzle(jdbcWrapper,
				localIdentityId, date, index, puzzle);
	}

	public synchronized void setPuzzleSolved(
			int localIdentityId, LocalDate date, int index) {

		IntroductionPuzzleImpl.setPuzzleSolved(jdbcWrapper,
				localIdentityId, date, index);
	}


	//----- IDENTITY INTRODUCTION -----//

	public synchronized List<IdentityIntroduction> getIdentityIntroductions(
			int localIdentityId, InsertStatus inserted) {

		return IdentityIntroductionImpl.getIdentityIntroductions(
				jdbcWrapper, localIdentityId, inserted);
	}

	public synchronized boolean isIntroductionPuzzledSolved(String uuid) {
		return IdentityIntroductionImpl.isIntroductionPuzzledSolved(
				jdbcWrapper, uuid);
	}

	public synchronized void saveIdentityIntroduction(int localIdentityId,
			LocalDate date, String uuid, String solution) {

		IdentityIntroductionImpl.saveIdentityIntroduction(jdbcWrapper,
				localIdentityId, date, uuid, solution);
	}


	public synchronized void setIdentityIntroductionInserted(
			int localIdentityId, LocalDate date, String uuid) {

		IdentityIntroductionImpl.setIdentityIntroductionInserted(
				jdbcWrapper, localIdentityId, date, uuid);
	}


	//----- INSERT TABLES -----//
	public synchronized int getInsertIndex(RequestType type,
			int localIdentityId,
			LocalDate date, InsertStatus inserted) {

		return InsertTablesImpl.getInsertIndex(jdbcWrapper,
				type, localIdentityId, date, inserted);
	}

	public synchronized void incrementInsertIndex(RequestType type,
			int localIdentityId,
			LocalDate date, int index) {
		InsertTablesImpl.incrementInsertIndex(jdbcWrapper,
				type, localIdentityId, date, index);
	}

	/**
	 * Update Insert History entry.
	 * Insert History tracks successful and pending inserts for each identity.
	 * If inserted is set to IGNORE and the entry already exists, it won't be
	 * changed. If a new entry is created, the value will be used.
	 * Not compatible with type MESSAGE.
	 * @param type request type
	 * @param localIdentityId numeric jfms.store ID of local identity
	 * @param date request date
	 * @param index request index
	 * @param inserted insert status (must be IGNORE if type is IDENTITY)
	 */
	public synchronized void updateInsert(RequestType type,
			Integer localIdentityId, LocalDate date, int index,
			InsertStatus inserted) {

		InsertTablesImpl.updateInsert(jdbcWrapper,
				type, localIdentityId, date, index, inserted);
	}

	//----- AVATAR -----//
	public synchronized void populateAvatarTable() {
		AvatarImpl.populateAvatarTable(jdbcWrapper);
	}

	public synchronized void updateAvatar(int identityId) {
		AvatarImpl.updateAvatar(jdbcWrapper, identityId);
	}

	public synchronized String getAvatarExtension(int identityId) {

		return AvatarImpl.getAvatarExtension(jdbcWrapper, identityId);
	}

	public synchronized void setAvatarExtension(int identityId,
			String extension) {

		AvatarImpl.setAvatarExtension(jdbcWrapper, identityId, extension);
	}

	public synchronized void setAvatarFailed(int identityId,
			LocalDate failDate) {

		AvatarImpl.setAvatarFailed(jdbcWrapper, identityId, failDate);
	}

	public synchronized void removeAvatar(int identityId) {
		AvatarImpl.removeAvatar(jdbcWrapper, identityId);
	}

	public synchronized List<Avatar> getMissingAvatars() {
		return AvatarImpl.getMissingAvatars(jdbcWrapper);
	}

	public synchronized boolean isAvatarDisabled(int identityId) {
		return AvatarImpl.isAvatarDisabled(jdbcWrapper, identityId);
	}

	public synchronized void setAvatarDisabled(int identityId,
			boolean enabled) {

		AvatarImpl.setAvatarDisabled(jdbcWrapper, identityId, enabled);
	}

	//----- DATABASE MAINTENANCE -----//
	public synchronized int countRemovableIdentities(LocalDate date,
			boolean inactiveOnly) {

		return MaintenanceImpl.countRemovableIdentities(jdbcWrapper,
				date, inactiveOnly);
	}

	public synchronized boolean removeIdentities(LocalDate date,
			boolean inactiveOnly) {

		return MaintenanceImpl.removeIdentities(jdbcWrapper,
				date, inactiveOnly);
	}

	//----- JFMS INFO -----//
	public synchronized String getValue(String key) {
		return JfmsInfoImpl.getValue(jdbcWrapper, key);
	}

	public synchronized void saveValue(String key, String value) {
		JfmsInfoImpl.saveValue(jdbcWrapper, key, value);
	}

	private void checkVersion() throws SQLException {
		String dbVersion = getValue(DB_VERSION_KEY);
		if (dbVersion == null) {
			LOG.log(Level.INFO, "JFMS Database version not set");
			saveValue(DB_VERSION_KEY, DB_VERSION);
			return;
		}

		LOG.log(Level.INFO, "Found JFMS Database version {0}", dbVersion);
		if (DB_VERSION.equals(dbVersion)) {
			return;
		}

		// update helper from
		// DB version 0.1 (jfms version <= 0.4) to
		// DB version 0.2 (jfms version 0.5+)
		if (Version.compare(dbVersion, "0.2") < 0) {
			LOG.log(Level.INFO, "Executing update statements for version 0.2");
			// add flags column to message table
			try (ResultSet rs = connection.getMetaData()
					.getColumns(null, null, "message", "flags")) {
				if (!rs.next()) {
					final String addColumn = "ALTER TABLE message "
						+ "ADD COLUMN flags INTEGER";
					try (Statement stmt = connection.createStatement()) {
						stmt.executeUpdate(addColumn);
					}
				}
			}
		}

		if (Version.compare(dbVersion, "0.3") < 0) {
			LOG.log(Level.INFO, "Executing update statements for version 0.3");
			// increase default seed trust from 50 to 90
			LocalTrustImpl.migrateSeedTrust(jdbcWrapper,
					Constants.DEFAULT_SEED_TRUST);
		}

		LOG.log(Level.INFO, "JFMS Database now at version {0}", DB_VERSION);
		saveValue(DB_VERSION_KEY, DB_VERSION);
	}
}
