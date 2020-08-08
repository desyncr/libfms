package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jfms.fms.IdentityIntroduction;

public class IdentityIntroductionImpl {
	static List<IdentityIntroduction> getIdentityIntroductions(
			JDBCWrapper jdbcWrapper,
			int localIdentityId, InsertStatus inserted) {

		StringBuilder str = new StringBuilder();
		str.append("SELECT insert_date, uuid, solution ");
		str.append("FROM identity_introduction_insert ");
		str.append("WHERE local_identity_id=?");

		switch (inserted) {
		case IGNORE:
			break;
		case INSERTED:
			str.append(" AND inserted=1");
			break;
		case NOT_INSERTED:
			str.append(" AND inserted=0");
			break;
		}

		return jdbcWrapper.executePreparedStatement(str.toString(),
				s -> handleGetIdentityIntroductions(s, localIdentityId),
				Collections.emptyList());
	}

	static List<IdentityIntroduction> handleGetIdentityIntroductions(
			PreparedStatement pstmt, int localIdentityId) throws SQLException {

		List<IdentityIntroduction> idIntros = new ArrayList<>();
		pstmt.setInt(1, localIdentityId);

		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			IdentityIntroduction idIntro = new IdentityIntroduction();
			idIntro.setDate(Utils.date(rs.getString(1)));
			idIntro.setUUID(rs.getString(2));
			idIntro.setSolution(rs.getString(3));

			idIntros.add(idIntro);
		}

		return idIntros;
	}

	static boolean isIntroductionPuzzledSolved(JDBCWrapper jdbcWrapper,
			String uuid) {

		final String selectIdIntro = "SELECT COUNT(*) "
			+ "FROM identity_introduction_insert "
			+ "WHERE uuid=?";

		return jdbcWrapper.executePreparedStatement(selectIdIntro,
				s -> handleIsIntroductionPuzzledSolved(s, uuid),
				false);
	}

	static boolean handleIsIntroductionPuzzledSolved(PreparedStatement pstmt,
			String uuid) throws SQLException {

		boolean solved = false;

		pstmt.setString(1, uuid);

		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			int solvedCount = rs.getInt(1);
			if (solvedCount > 0) {
				solved = true;
			}
		}

		return solved;
	}

	static boolean saveIdentityIntroduction(JDBCWrapper jdbcWrapper,
			int localIdentityId, LocalDate date,
			String uuid, String solution) {

		final String insertIdIntro = "INSERT INTO identity_introduction_insert"
			+ "(local_identity_id, insert_date, uuid, solution) "
			+ "VALUES(?,?,?,?)";

		return jdbcWrapper.executePreparedStatement(insertIdIntro,
				(PreparedStatement pstmt) -> {
					pstmt.setInt(1, localIdentityId);
					pstmt.setString(2, Utils.format(date));
					pstmt.setString(3, uuid);
					pstmt.setString(4, solution);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	static boolean setIdentityIntroductionInserted(JDBCWrapper jdbcWrapper,
			int localIdentityId, LocalDate date, String uuid) {

		final String insertPuzzle = "UPDATE identity_introduction_insert "
			+ "SET INSERTED=1 "
			+ "WHERE local_identity_id=? AND insert_date=? AND uuid=?";

		return jdbcWrapper.executePreparedStatement(insertPuzzle,
				(PreparedStatement pstmt) -> {
					pstmt.setInt(1, localIdentityId);
					pstmt.setString(2, Utils.format(date));
					pstmt.setString(3, uuid);
					pstmt.executeUpdate();
					return true;
				},
				false);
	}

	private IdentityIntroductionImpl() {
	}
}
