package jfms.store;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface StatementHandler<T> {
	T getResult(ResultSet stmt) throws SQLException;
}
