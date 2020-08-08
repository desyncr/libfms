package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import jfms.fms.DateIndex;
import jfms.fms.IntroductionPuzzle;

public class IntroductionPuzzleImpl {
	static Map<DateIndex, IntroductionPuzzle> getUnsolvedPuzzles(
			JDBCWrapper jdbcWrapper, int localIdentityId, LocalDate fromDate) {

		final String selectPuzzle = "SELECT insert_date, insert_index, uuid, solution "
			+ "FROM introduction_puzzle "
			+ "WHERE local_identity_id=? AND insert_date>=? AND solved=0";

		return jdbcWrapper.executePreparedStatement(selectPuzzle,
				s -> handleGetUnsolvedPuzzles(s, localIdentityId, fromDate),
				Collections.emptyMap());
	}

	static Map<DateIndex, IntroductionPuzzle> handleGetUnsolvedPuzzles(
			PreparedStatement pstmt, int localIdentityId, LocalDate fromDate)
		throws SQLException {

		Map<DateIndex, IntroductionPuzzle> puzzles = new HashMap<>();

		pstmt.setInt(1, localIdentityId);
		pstmt.setString(2, Utils.format(fromDate));

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			LocalDate date = Utils.date(rs.getString(1));
			int index = rs.getInt(2);
			String uuid = rs.getString(3);
			String solution = rs.getString(4);

			IntroductionPuzzle puzzle = new IntroductionPuzzle();
			puzzle.setUUID(uuid);
			puzzle.setSolution(solution);
			puzzles.put(new DateIndex(date, index), puzzle);
		}

		return puzzles;
	}

	static int getUnsolvedPuzzleCount(JDBCWrapper jdbcWrapper,
			int localIdentityId, LocalDate date) {

		final String selectPuzzle = "SELECT COUNT(*) "
			+ "FROM introduction_puzzle "
			+ "WHERE local_identity_id=? AND insert_date=? AND solved=0";

		return jdbcWrapper.executePreparedStatement(selectPuzzle,
			s -> handleGetUnsolvedPuzzleCount(s, localIdentityId, date),
			0);
	}

	static int handleGetUnsolvedPuzzleCount(PreparedStatement pstmt,
			int localIdentityId, LocalDate date) throws SQLException {

		int count = 0;

		pstmt.setInt(1, localIdentityId);
		pstmt.setString(2, Utils.format(date));

		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			count = rs.getInt(1);
		}

		return count;
	}

	static int getNextIntroductionPuzzleIndex(
			JDBCWrapper jdbcWrapper, int localIdentityId, LocalDate date) {

		final String selectPuzzle = "SELECT MAX(insert_index) "
			+ "FROM introduction_puzzle "
			+ "WHERE local_identity_id=? AND insert_date=?";

		return jdbcWrapper.executePreparedStatement(selectPuzzle,
				s -> handleGetNextIntroductionPuzzleIndex(s, localIdentityId, date),
				0);
	}

	static int handleGetNextIntroductionPuzzleIndex(PreparedStatement pstmt,
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

		return maxIndex + 1;
	}

	static boolean saveIntroductionPuzzle(JDBCWrapper jdbcWrapper,
			int localIdentityId, LocalDate date, int index,
			IntroductionPuzzle puzzle) {

		final String insertPuzzle = "INSERT INTO introduction_puzzle "
			+ "(local_identity_id, insert_date, insert_index, uuid, solution) "
			+ "VALUES(?,?,?,?,?)";

		return jdbcWrapper.executePreparedStatement(insertPuzzle,
				s -> handleSaveIntroductionPuzzle(s, localIdentityId,
					date, index, puzzle),
				false);
	}

	static boolean handleSaveIntroductionPuzzle(PreparedStatement pstmt,
			int localIdentityId, LocalDate date, int index,
			IntroductionPuzzle puzzle) throws SQLException {

		pstmt.setInt(1, localIdentityId);
		pstmt.setString(2, Utils.format(date));
		pstmt.setInt(3, index);
		pstmt.setString(4, puzzle.getUuid());
		pstmt.setString(5, puzzle.getSolution());

		pstmt.executeUpdate();

		return true;
	}

	static boolean setPuzzleSolved(JDBCWrapper jdbcWrapper,
			int localIdentityId, LocalDate date, int index) {
		final String selectPuzzle = "UPDATE introduction_puzzle "
			+ "SET solved=1 "
			+ "WHERE local_identity_id=? AND insert_date=? AND insert_index=?";

		return jdbcWrapper.executePreparedStatement(selectPuzzle,
				(PreparedStatement pstmt) -> {
					pstmt.setInt(1, localIdentityId);
					pstmt.setString(2, Utils.format(date));
					pstmt.setInt(3, index);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	private IntroductionPuzzleImpl() {
	}
}
