package jfms.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JDBCWrapper {
	private static final Logger LOG = Logger.getLogger(JDBCWrapper.class.getName());
	private static final boolean LOG_QUERIES = false;

	private final Connection connection;

	public JDBCWrapper(Connection connection) {
		this.connection = connection;
	}

	public Statement createStatement(String query) throws SQLException {
		if (LOG_QUERIES) {
			LOG.log(Level.FINEST, "Creating SQL query: {0}", query);
		}

		return connection.createStatement();
	}

	public PreparedStatement prepareStatement(String query)
		throws SQLException {

		if (LOG_QUERIES) {
			LOG.log(Level.FINEST, "Preparing SQL query: {0}", query);
		}

		return connection.prepareStatement(query);
	}

	public <T> T executeStatement(String query, StatementHandler<T> handler,
			T resultOnException) {

		if (LOG_QUERIES) {
			LOG.log(Level.FINEST, "Executing SQL query: {0}", query);
		}

		try (Statement stmt = connection.createStatement()) {
			ResultSet rs = stmt.executeQuery(query);
			return handler.getResult(rs);
		} catch (SQLException e) {
			Utils.logSqlException("SQL query failed", e);
		}

		return resultOnException;
	}

	public <T> T executePreparedStatement(String query, PreparedStatementHandler<T> handler)
		throws SQLException {

		if (LOG_QUERIES) {
			LOG.log(Level.FINEST, "Executing preprated SQL query: {0}", query);
		}

		try (PreparedStatement pstmt = connection.prepareStatement(query)) {
			return handler.getResult(pstmt);
		}
	}

	public <T> T executePreparedStatement(String query, PreparedStatementHandler<T> handler,
			T resultOnException) {

		if (LOG_QUERIES) {
			LOG.log(Level.FINEST, "Executing preprated SQL query: {0}", query);
		}

		try (PreparedStatement pstmt = connection.prepareStatement(query)) {
			return handler.getResult(pstmt);
		} catch (SQLException e) {
			Utils.logSqlException("SQL query failed", e);
		}

		return resultOnException;
	}

	public <T> T executeTransaction(TransactionHandler<T> handler,
			T resultOnException) {

		if (LOG_QUERIES) {
			LOG.log(Level.FINEST, "Begin SQL transaction");
		}

		try {
			connection.setAutoCommit(false);

			T result = handler.getResult(this);

			connection.commit();
			return result;
		} catch (SQLException e) {
			try {
				Utils.logSqlException("SQL query failed", e);
				connection.rollback();
			} catch (SQLException ex) {
				Utils.logSqlException("rollback failed", ex);
			}
		} finally {
			try {
				if (LOG_QUERIES) {
					LOG.log(Level.FINEST, "End SQL transaction");
				}

				connection.setAutoCommit(true);
			} catch (SQLException ex) {
				Utils.logSqlException("failed to enable autocommit", ex);
			}
		}

		return resultOnException;
	}
}
