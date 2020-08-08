package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BoardImpl {
	private static final String INSERT_BOARD =
		"INSERT INTO board(name, subscribed) VALUES(?,?)";
	static Map<String, Integer> getBoardNames(JDBCWrapper jdbcWrapper) {
		final String selectBoards = "SELECT board_id, name "
			+ "FROM board";

		return jdbcWrapper.executeStatement(selectBoards,
				BoardImpl::handleGetBoardNames,
				Collections.emptyMap());
	}

	static Map<String, Integer> handleGetBoardNames(ResultSet rs)
		throws SQLException {

		Map<String,Integer> boardNames = new HashMap<>();

		while (rs.next()) {
			int boardId = rs.getInt(1);
			String name = rs.getString(2);
			boardNames.put(name, boardId);
		}

		return boardNames;
	}

	static Map<String, Integer> getBoardInfos(JDBCWrapper jdbcWrapper) {
		final String selectBoards = "SELECT b.name, COUNT(bm.message_id) "
			+ "FROM board b LEFT OUTER JOIN board_message bm "
			+ "ON (b.board_id = bm.board_id) "
			+ "GROUP BY b.board_id";

		return jdbcWrapper.executeStatement(selectBoards,
				BoardImpl::handleGetBoardInfos,
				Collections.emptyMap());
	}

	static Map<String, Integer> handleGetBoardInfos(ResultSet rs)
		throws SQLException {

		Map<String,Integer> boardInfos = new HashMap<>();

		while (rs.next()) {
			String name = rs.getString(1);
			int messageCount = rs.getInt(2);
			boardInfos.put(name, messageCount);
		}

		return boardInfos;
	}

	static List<String> getSubscribedBoardNames(JDBCWrapper jdbcWrapper) {
		final String selectBoards = "SELECT name "
			+ "FROM board "
			+ "WHERE subscribed = 1 "
			+ "ORDER BY name";

		return jdbcWrapper.executeStatement(selectBoards,
				BoardImpl::handleGetSubscribedBoardNames,
				Collections.emptyList());
	}

	static List<String> handleGetSubscribedBoardNames(
			ResultSet rs) throws SQLException {

		List<String> boardNames = new ArrayList<>();

		while (rs.next()) {
			String name = rs.getString(1);
			boardNames.add(name);
		}

		return boardNames;
	}

	static boolean setBoardSubscribed(JDBCWrapper jdbcWrapper,
			int boardId, boolean subscribed) {

		final String updateBoard = "UPDATE board "
			+ "SET subscribed=? "
			+ "WHERE board_id=?";

		return jdbcWrapper.executePreparedStatement(updateBoard,
			(PreparedStatement pstmt) -> {
				pstmt.setBoolean(1, subscribed);
				pstmt.setInt(2, boardId);
				pstmt.executeUpdate();
				return true;
			},
			false);
	}

	static int getUnreadMessageCount(JDBCWrapper jdbcWrapper, int boardId) {
		final String selectMessages = "SELECT COUNT(*) "
			+ "FROM message m "
			+ "INNER JOIN board_message bm ON (m.message_id = bm.message_id) "
			+ "WHERE bm.board_id=? AND m.read = 0";

		return jdbcWrapper.executePreparedStatement(selectMessages,
				s -> handleGetUnreadMessageCount(s, boardId),
				0);
	}

	static int handleGetUnreadMessageCount(PreparedStatement pstmt,
			int boardId) throws SQLException {

		int count = 0;

		pstmt.setInt(1, boardId);

		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			count = rs.getInt(1);
		}

		return count;
	}

	static int saveBoard(JDBCWrapper jdbcWrapper,
			String boardName, boolean subscribed) {

		return jdbcWrapper.executePreparedStatement(INSERT_BOARD,
				s-> handleSaveBoard(s, boardName, subscribed),
				-1);
	}

	static int saveBoardInternal(JDBCWrapper jdbcWrapper,
			String boardName, boolean subscribed)
		throws SQLException {

		return jdbcWrapper.executePreparedStatement(INSERT_BOARD,
				s-> handleSaveBoard(s, boardName, subscribed));
	}

	static int handleSaveBoard(PreparedStatement pstmt,
			String boardName, boolean subscribed) throws SQLException {

		pstmt.setString(1, boardName);
		pstmt.setBoolean(2, subscribed);
		pstmt.executeUpdate();

		ResultSet rs = pstmt.getGeneratedKeys();
		int boardId = -1;
		if (rs.next()) {
			boardId = rs.getInt(1);
		}

		if (boardId == -1) {
			throw new SQLException("failed to get board ID");
		}

		return boardId;
	}

	private BoardImpl() {
	}
}
