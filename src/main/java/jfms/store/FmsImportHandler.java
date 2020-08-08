package jfms.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface FmsImportHandler {
	void run(ResultSet rs, PreparedStatement insertStmt) throws SQLException;
}
