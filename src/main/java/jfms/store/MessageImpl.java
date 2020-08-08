package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import jfms.fms.Attachment;
import jfms.fms.BoardManager;
import jfms.fms.FmsManager;
import jfms.fms.IdentityManager;
import jfms.fms.InReplyTo;
import jfms.fms.Message;
import jfms.fms.MessageReference;

public class MessageImpl {
	private static final Logger LOG = Logger.getLogger(MessageImpl.class.getName());

	private static final String MSG_NOT_FOUND_TEXT =
		"Failed to retrieve Message body";

	static Message getMessage(JDBCWrapper jdbcWrapper, int messageId) {
		return jdbcWrapper.executeTransaction(
				w -> handleGetMessage(w, messageId),
				null);
	}

	static Message handleGetMessage(JDBCWrapper jdbcWrapper, int messageId)
		throws SQLException {

		Message message = null;

		final String selectMessage = "SELECT m.identity_id, "
			+ "m.date, m.time, m.subject, m.message_uuid, m.reply_board_id, "
			+ "m.body "
			+ "FROM message m "
			+ "WHERE m.message_id=?";
		final String selectBoard = "SELECT board_id "
			+ "FROM board_message "
			+ "WHERE message_id=?";
		final String selectInReplyTo = "SELECT reply_order, message_uuid "
			+ "FROM message_reply_to "
			+ "WHERE message_id=?";
		BoardManager boardManager = FmsManager.getInstance().getBoardManager();

		try (PreparedStatement pstmt =
				jdbcWrapper.prepareStatement(selectMessage)) {
			pstmt.setInt(1, messageId);

			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				message = new Message();
				message.setIdentityId(rs.getInt(1));
				message.setDate(Utils.date(rs.getString(2)));
				message.setTime(Utils.time(rs.getString(3)));
				message.setSubject(rs.getString(4));
				message.setMessageUuid(rs.getString(5));
				int boardId = rs.getInt(6);
				message.setBody(rs.getString(7));

				String boardName = boardManager.getBoardName(boardId);
				if (boardName == null) {
					LOG.log(Level.WARNING, "Board with ID={0} not found",
							boardId);
					return null;
				}
				message.setReplyBoard(boardName);
			}
		}

		if (message == null) {
			LOG.log(Level.WARNING, "message with ID={0} not found",
					messageId);
			return null;
		}

		List<String> boards = new ArrayList<>();
		try (PreparedStatement pstmt =
				jdbcWrapper.prepareStatement(selectBoard)) {
			pstmt.setInt(1, messageId);

			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int boardId = rs.getInt(1);
				String boardName = boardManager.getBoardName(boardId);
				if (boardName != null) {
					boards.add(boardName);
				} else {
					LOG.log(Level.WARNING, "Board with ID={0} not found",
							boardId);
				}
			}
		}
		message.setBoards(boards);

		InReplyTo inReplyTo = new InReplyTo();
		try (PreparedStatement pstmt =
				jdbcWrapper.prepareStatement(selectInReplyTo)) {
			pstmt.setInt(1, messageId);

			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int order = rs.getInt(1);
				String messageUuid = rs.getString(2);
				inReplyTo.add(order, messageUuid);
			}
		}
		message.setInReplyTo(inReplyTo);

		return message;
	}

	static String getMessageBody(JDBCWrapper jdbcWrapper, int messageId) {
		final String selectMessage = "SELECT body "
			+ "FROM message "
			+ "WHERE message_id=?";

		return jdbcWrapper.executePreparedStatement(selectMessage,
				s -> handleGetMessageBody(s, messageId),
				MSG_NOT_FOUND_TEXT);
	}

	static String handleGetMessageBody(PreparedStatement pstmt, int messageId)
		throws SQLException {

		pstmt.setInt(1, messageId);
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			return rs.getString(1);
		}

		return MSG_NOT_FOUND_TEXT;
	}

	static List<Attachment> getAttachments(JDBCWrapper jdbcWrapper,
			int messageId) {

		final String selectAttachment = "SELECT uri, size "
			+ "FROM attachment "
			+ "WHERE message_id=?";

		return jdbcWrapper.executePreparedStatement(selectAttachment,
				s -> handleGetAttachments(s, messageId),
				Collections.emptyList());
	}

	static List<Attachment> handleGetAttachments(PreparedStatement pstmt,
			int messageId) throws SQLException {

		List<Attachment> attachments = new ArrayList<>();

		pstmt.setInt(1, messageId);

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			String uri = rs.getString(1);
			int size = rs.getInt(2);
			attachments.add(new Attachment(uri, size));
		}

		return attachments;
	}

	static boolean messageExists(JDBCWrapper jdbcWrapper, int identityId,
			LocalDate insertDate, int insertIndex) {

		final String selectMessage = "SELECT COUNT(*) "
			+ "FROM message "
			+ "WHERE identity_id=? AND insert_date=? AND insert_index=?";

		return jdbcWrapper.executePreparedStatement(selectMessage,
				s -> handleMessageExists(s, identityId, insertDate, insertIndex),
				false);
	}

	static boolean handleMessageExists(PreparedStatement pstmt,
			int identityId, LocalDate insertDate, int insertIndex)
		throws SQLException {

		pstmt.setInt(1, identityId);
		pstmt.setString(2, Utils.format(insertDate));
		pstmt.setInt(3, insertIndex);

		ResultSet rs = pstmt.executeQuery();
		boolean exists = false;
		if (rs.next()) {
			if (rs.getInt(1) > 0) {
				exists = true;
			}
		}

		return exists;
	}

	static int saveMessage(JDBCWrapper jdbcWrapper, Message message,
			Map<Integer,String> newBoards) {

		return jdbcWrapper.executeTransaction(
				w -> handleSaveMessage(w, message, newBoards),
				-1);
	}

	static int handleSaveMessage(JDBCWrapper jdbcWrapper, Message message,
			Map<Integer,String> newBoards) throws SQLException {

		final String selectMessage = "SELECT COUNT(*) "
			+ "FROM message "
			+ "WHERE message_uuid=?";
		final String insertMessage = "INSERT INTO message "
			+ "(identity_id, date, time, subject, message_uuid, reply_board_id, "
			+ "insert_date, insert_index, body) "
			+ "VALUES(?,?,?,?,?,?,?,?,?)";


		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(selectMessage)) {
			pstmt.setString(1, message.getMessageUuid());

			boolean exists = false;
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				if (rs.getInt(1) > 0) {
					exists = true;
				}
			}

			if (exists) {
				LOG.log(Level.INFO, "Message with UUID {0} already exists",
						message.getMessageUuid());
				return -1;
			}
		}

		final String replyBoard = message.getReplyBoard();
		BoardManager boardManager = FmsManager.getInstance().getBoardManager();
		Integer replyBoardId = boardManager.getBoardId(replyBoard);
		if (replyBoardId == null) {
			replyBoardId = BoardImpl.saveBoardInternal(jdbcWrapper,
					replyBoard, false);
			if (newBoards != null) {
				newBoards.put(replyBoardId, replyBoard);
			}
		}

		int messageId = -1;
		try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(insertMessage)) {
			pstmt.setInt(1, message.getIdentityId());
			pstmt.setString(2, Utils.format(message.getDate()));
			pstmt.setString(3, Utils.format(message.getTime()));
			pstmt.setString(4, message.getSubject());
			pstmt.setString(5, message.getMessageUuid());
			pstmt.setInt(6, replyBoardId);
			pstmt.setString(7, Utils.format(message.getInsertDate()));
			pstmt.setInt(8, message.getInsertIndex());
			pstmt.setString(9, message.getBody());

			pstmt.executeUpdate();

			ResultSet rs = pstmt.getGeneratedKeys();
			if (rs.next()) {
				messageId = rs.getInt(1);
			}
		}

		if (messageId == -1) {
			throw new SQLException("failed to get message ID");
		}

		final String insertMessageBoard = "INSERT INTO board_message "
			+ " (board_id, message_id) VALUES(?,?)";
		final List<String> boards = message.getBoards();


		final List<Integer> boardIds = new ArrayList<>();
		if (boards != null) {
			for (String b : boards) {
				if (b.equals(replyBoard)) {
					boardIds.add(replyBoardId);
					continue;
				}

				Integer boardId = boardManager.getBoardId(b);
				if (boardId == null) {
					boardId = BoardImpl.saveBoardInternal(jdbcWrapper, b, false);
					if (newBoards != null) {
						newBoards.put(boardId, b);
					}
				}
				boardIds.add(boardId);
			}
		}

		if (!boardIds.isEmpty()) {
			try (PreparedStatement pstmt =
					jdbcWrapper.prepareStatement(insertMessageBoard)) {
				for (Integer boardId : boardIds) {
					pstmt.setInt(1, boardId);
					pstmt.setInt(2, messageId);
					pstmt.addBatch();
				}
				pstmt.executeBatch();
			}
		}

		final InReplyTo inReplyTo = message.getInReplyTo();
		if (inReplyTo != null /*&& !inReplyTo.isEmpty()*/) {
			final String insertReplyTo = "INSERT INTO message_reply_to "
				+ "(message_id, reply_order, message_uuid) "
				+ "VALUES(?,?,?)";
			try (PreparedStatement pstmt =
					jdbcWrapper.prepareStatement(insertReplyTo)) {
				pstmt.setInt(1, messageId);
				for (Map.Entry<Integer, String> e : inReplyTo.getMessages().entrySet()) {
					pstmt.setInt(2, e.getKey());
					pstmt.setString(3, e.getValue());
					pstmt.addBatch();
				}
				pstmt.executeBatch();
			}
		}

		final List<Attachment> attachments = message.getAttachments();
		if (attachments != null) {
			final String insertAttachment = "INSERT INTO attachment "
				+ "(message_id, uri, size) VALUES(?,?,?)";
			try (PreparedStatement pstmt =
					jdbcWrapper.prepareStatement(insertAttachment)) {
				for (Attachment a : attachments) {
					pstmt.setInt(1, messageId);
					pstmt.setString(2, a.getKey());
					pstmt.setInt(3, a.getSize());
					pstmt.addBatch();
				}
				pstmt.executeBatch();
			}
		}

		return messageId;
	}

	static boolean removeMessage(JDBCWrapper jdbcWrapper, int messageId) {
		return jdbcWrapper.executeTransaction(
				w -> handleRemoveMessage(w, messageId),
				false);
	}

	static boolean handleRemoveMessage(JDBCWrapper jdbcWrapper, int messageId)
		throws SQLException {

		final String[] deleteQueries = new String[] {
			"DELETE FROM message WHERE message_id=?",
			"DELETE FROM attachment WHERE message_id=?",
			"DELETE FROM board_message WHERE message_id=?",
			"DELETE FROM message_reply_to WHERE message_id=?"
		};

		for (String q : deleteQueries) {
			try (PreparedStatement pstmt = jdbcWrapper.prepareStatement(q)) {
				pstmt.setInt(1, messageId);
				pstmt.executeUpdate();
			}
		}

		return true;
	}

	static boolean setMessageRead(JDBCWrapper jdbcWrapper,
			int messageId, boolean read) {

		final String updateMessage = "UPDATE message "
			+ "SET read=? "
			+ "WHERE message_id=?";

		return jdbcWrapper.executePreparedStatement(updateMessage,
				(PreparedStatement pstmt) -> {
					pstmt.setBoolean(1, read);
					pstmt.setInt(2, messageId);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	static boolean setBoardMessagesRead(JDBCWrapper jdbcWrapper,
			int boardId, boolean read) {

		final String updateMessage = "UPDATE message "
			+ "SET read=? "
			+ "WHERE message.read=? AND EXISTS "
			+ "(SELECT * FROM board_message bm WHERE bm.board_id=? AND message.message_id = bm.message_id)";

		return jdbcWrapper.executePreparedStatement(updateMessage,
				(PreparedStatement pstmt) -> {
					pstmt.setBoolean(1, read);
					pstmt.setBoolean(2, !read);
					pstmt.setInt(3, boardId);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	static boolean setAllMessagesRead(JDBCWrapper jdbcWrapper) {
		final String updateMessage = "UPDATE message "
			+ "SET read=1 "
			+ "WHERE message.read=0";

		return jdbcWrapper.executePreparedStatement(updateMessage,
				(PreparedStatement pstmt) -> {
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	static boolean setMessageFlags(JDBCWrapper jdbcWrapper,
			int messageId, int flags) {

		final String updateMessage = "UPDATE message "
			+ "SET flags=? "
			+ "WHERE message_id=?";

		return jdbcWrapper.executePreparedStatement(updateMessage,
				(PreparedStatement pstmt) -> {
					pstmt.setInt(1, flags);
					pstmt.setInt(2, messageId);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	static int getMessageCount(JDBCWrapper jdbcWrapper, int identityId) {
		final String selectMessages = "SELECT COUNT(*) "
			+ "FROM message m "
			+ "WHERE identity_id=?";

		return jdbcWrapper.executePreparedStatement(selectMessages,
				s -> handleGetMessageCount(s, identityId),
				0);
	}

	static int handleGetMessageCount(PreparedStatement pstmt,
			int identityId) throws SQLException {

		int count = 0;

		pstmt.setInt(1, identityId);

		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			count = rs.getInt(1);
		}

		return count;
	}

	static List<MessageReference> getExternalMessageList(
			JDBCWrapper jdbcWrapper,int identityId, int limit) {

		final String selectMessages = "SELECT m.message_id, "
			+ "m.identity_id, m.insert_date, m.insert_index, "
			+ "bm.board_id "
			+ "FROM message m "
			+ "INNER JOIN board_message bm ON (m.message_id = bm.message_id) "
			+ "WHERE m.identity_id<>? "
			+ "ORDER BY m.date DESC, m.time DESC LIMIT ?";

		return jdbcWrapper.executePreparedStatement(selectMessages,
				s -> handleGetExternalMessageList(s, identityId, limit),
				Collections.emptyList());
	}

	static List<MessageReference> handleGetExternalMessageList(
			PreparedStatement pstmt, int identityId, int limit)
		throws SQLException {

		BoardManager boardManager = FmsManager.getInstance().getBoardManager();
		IdentityManager identityManager = FmsManager.getInstance().getIdentityManager();
		Map<Integer, MessageReference> messageList = new HashMap<>();

		pstmt.setInt(1, identityId);
		pstmt.setInt(2, limit);

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			int messageId = rs.getInt(1);
			MessageReference msgRef = messageList.get(messageId);
			if (msgRef == null) {
				msgRef = new MessageReference();
				msgRef.setBoards(new ArrayList<>());
				messageList.put(messageId, msgRef);
			}

			int msgIdentityId = rs.getInt(2);
			msgRef.setIdentityId(msgIdentityId);

			String ssk = identityManager.getSsk(msgIdentityId);
			if (ssk != null) {
				msgRef.setSsk(ssk);
			} else {
				LOG.log(Level.WARNING, "failed to get SSK of ID {0}",
						msgIdentityId);
			}

			final LocalDate date = Utils.date(rs.getString(3));
			msgRef.setDate(date);

			msgRef.setIndex(rs.getInt(4));
			int boardId = rs.getInt(5);

			String boardName = boardManager.getBoardName(boardId);
			if (boardName != null) {
				msgRef.getBoards().add(boardName);
			} else {
				LOG.log(Level.WARNING, "failed to get name of board {0}",
						boardId);
			}

			if (msgRef.getSsk() == null || msgRef.getBoards().isEmpty()) {
				// error while resolving SSK or board name -> remove
				messageList.remove(messageId);
			}
		}

		return messageList.entrySet().stream()
			.map(Map.Entry::getValue)
			.collect(Collectors.toList());
	}

	private MessageImpl() {
	}
}
