package jfms.store;

import java.sql.SQLException;

@FunctionalInterface
public interface TransactionHandler<T> {
	T getResult(JDBCWrapper jdbcWrapper) throws SQLException;
}
