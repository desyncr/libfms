package jfms.store;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {
	private static final Logger LOG = Logger.getLogger(Utils.class.getName());

	public static void logSqlException(String message, SQLException ex) {
		for (Throwable e : ex) {
			if (e instanceof SQLException) {
				LOG.log(Level.WARNING, message, e);
				LOG.log(Level.WARNING, "SQLState: {0}", ((SQLException)e).getSQLState());

				LOG.log(Level.WARNING, "Error Code: {0}", ((SQLException)e).getErrorCode());

				LOG.log(Level.WARNING, "Message: {0}", e.getMessage());

				Throwable t = ex.getCause();
				while (t != null) {
					LOG.log(Level.WARNING, "Cause", t);
					t = t.getCause();
				}
			}
		}
	}

	public static String format(LocalDate date) {
		return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
	}

	public static String format(LocalTime time) {
		return time.format(DateTimeFormatter.ISO_LOCAL_TIME);
	}

	public static LocalDate date(String dateStr) {
		return LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
	}

	public static LocalTime time(String timeStr) {
		return LocalTime.parse(timeStr, DateTimeFormatter.ISO_LOCAL_TIME);
	}

	private Utils() {
	}
}
