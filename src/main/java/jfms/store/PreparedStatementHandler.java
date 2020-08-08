package jfms.store;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface PreparedStatementHandler<T> {
	T getResult(PreparedStatement pstmt) throws SQLException;
}
