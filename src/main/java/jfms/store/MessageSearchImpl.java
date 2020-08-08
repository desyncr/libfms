package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.FmsManager;
import jfms.fms.Message;

public class MessageSearchImpl {
	private static final Logger LOG = Logger.getLogger(MessageSearchImpl.class.getName());

	static List<Message> getMessagesForBoard(JDBCWrapper jdbcWrapper,
			String board) {

		MessageSearchCriteria msc = new MessageSearchCriteria();
		msc.setBoard(board);

		return jdbcWrapper.executePreparedStatement(msc.getSQL(),
			w -> handleGetMessages(w, msc),
			Collections.emptyList());
	}

	static List<Message> getRecentMessages(JDBCWrapper jdbcWrapper,
			boolean subscribedOnly) {

		MessageSearchCriteria msc = new MessageSearchCriteria();
		msc.setRecentCount(50);
		msc.setSubscribedOnly(subscribedOnly);

		return jdbcWrapper.executePreparedStatement(msc.getSQL(),
			w -> handleGetMessages(w, msc),
			Collections.emptyList());
	}

	static List<Message> getStarredMessages(JDBCWrapper jdbcWrapper) {
		MessageSearchCriteria msc = new MessageSearchCriteria();
		msc.setFlags(Constants.MSG_FLAG_STARRED);

		return jdbcWrapper.executePreparedStatement(msc.getSQL(),
			w -> handleGetMessages(w, msc),
			Collections.emptyList());
	}

	static List<Message> findMessages(JDBCWrapper jdbcWrapper,
			MessageSearchCriteria msc) {

		return jdbcWrapper.executePreparedStatement(msc.getSQL(),
			w -> handleGetMessages(w, msc),
			Collections.emptyList());
	}

	static List<Message> handleGetMessages(PreparedStatement pstmt,
			MessageSearchCriteria msc) throws SQLException {

		msc.setParameters(pstmt);

		List<Message> messages = new ArrayList<>();
		Map<Integer, Message> messageMap = new HashMap<>();

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			final int messageId = rs.getInt(1);
			Message m = messageMap.get(messageId);
			if (m == null) {
				m = new Message();
				m.setMessageId(messageId);
				m.setIdentityId(rs.getInt(2));
				m.setInsertDate(Utils.date(rs.getString(3)));
				m.setInsertIndex(rs.getInt(4));
				if (rs.wasNull()) {
					m.setInsertIndex(-1);
				}
				m.setDate(Utils.date(rs.getString(5)));
				m.setTime(Utils.time(rs.getString(6)));
				m.setSubject(rs.getString(7));
				m.setMessageUuid(rs.getString(8));
				m.setReplyBoard(getBoardName(rs.getInt(9)));
				m.setRead(rs.getBoolean(10));
				m.setFlags(rs.getInt(11));
				m.setParentId(rs.getString(12));


				messageMap.put(m.getMessageId(), m);
				messages.add(m);
			}

			String board = getBoardName(rs.getInt(13));
			List<String> boards = m.getBoards();
			if (boards == null) {
				boards = new ArrayList<>(1);
				m.setBoards(boards);
			}

			boards.add(board);
		}

		return messages;
	}
	private static String getBoardName(int boardId) {
		String name = FmsManager.getInstance().getBoardManager()
			.getBoardName(boardId);
		if (name == null) {
			LOG.log(Level.WARNING, "unknown board {0}", boardId);
			name = "<unknown>";
		}

		return name;
	}

	private MessageSearchImpl() {
	}
}
