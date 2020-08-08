package jfms.store;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.fms.FmsManager;

public class MessageSearchCriteria {
	private static final Logger LOG = Logger.getLogger(MessageSearchCriteria.class.getName());

	private int boardId = -1;
	private int identityId = -1;
	private String from;
	private String uuid;
	private String subject;
	private String body;
	private int recentCount = -1;
	private boolean subscribedOnly = false;
	private int flags = 0;

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();

		if (from != null) {
			str.append("From=");
			str.append(from);
			str.append('\n');
		}

		if (uuid != null) {
			str.append("UUID=");
			str.append(uuid);
			str.append('\n');
		}

		if (subject != null) {
			str.append("Subject=");
			str.append(subject);
			str.append('\n');
		}

		if (boardId != -1) {
			final String boardName = FmsManager.getInstance()
				.getBoardManager().getBoardName(boardId);
			str.append("Board=");
			str.append(boardName);
			str.append('\n');
		}

		if (body != null) {
			str.append("Body=");
			str.append(body);
			str.append('\n');
		}

		return str.toString();
	}

	public void setBoard(String board) {
		Integer id = FmsManager.getInstance().getBoardManager()
			.getBoardId(board);

		if (id != null) {
			boardId = id;
		} else {
			LOG.log(Level.WARNING, "Skipping unknown board {0}", board);
			boardId = 0;
		}
	}

	public void setIdentityId(int identityId) {
		this.identityId = identityId;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public void setUUID(String uuid) {
		this.uuid = uuid;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public void setRecentCount(int recentCount) {
		this.recentCount = recentCount;
	}

	public void setSubscribedOnly(boolean subscribedOnly) {
		this.subscribedOnly = subscribedOnly;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	public String getSQL() {
		StringBuilder str = new StringBuilder(
			"SELECT m.message_id, m.identity_id, "
			+ "m.insert_date, m.insert_index, m.date, m.time, m.subject, "
			+ "m.message_uuid, m.reply_board_id, "
			+ "m.read, m.flags, rt.message_uuid, bm.board_id ");
		if (identityId == -1 && from != null) {
			str.append(", i.name ");
		}

		str.append("FROM message m "
			+ "LEFT OUTER JOIN message_reply_to rt ON (m.message_id = rt.message_id) "
			+ "LEFT OUTER JOIN board_message bm ON (m.message_id = bm.message_id) ");
		if (identityId == -1 && from != null) {
			str.append("LEFT OUTER JOIN identity i ON (m.identity_id = i.identity_id) ");
		}
		if (subscribedOnly) {
			str.append("LEFT OUTER JOIN board b ON (bm.board_id = b.board_id) ");
		}

		str.append("WHERE ");

		int conditionCount = 0;
		if (identityId != -1) {
			str.append("m.identity_id=? ");
			conditionCount++;
		} else if (from != null) {
			str.append("i.name LIKE ? ESCAPE '\\' ");
			conditionCount++;
		}

		if (uuid != null) {
			if (conditionCount > 0) {
				str.append("AND ");
			}
			str.append("m.message_uuid=?");
			conditionCount++;
		}

		if (subject != null) {
			if (conditionCount > 0) {
				str.append("AND ");
			}
			str.append("m.subject LIKE ? ESCAPE '\\' ");
			conditionCount++;
		}

		if (body != null) {
			if (conditionCount > 0) {
				str.append("AND ");
			}
			str.append("m.body LIKE ? ESCAPE '\\' ");

			conditionCount++;
		}

		if (subscribedOnly) {
			if (conditionCount > 0) {
				str.append("AND ");
			}
			str.append("b.subscribed=1 ");

			conditionCount++;
		}

		if (flags > 0) {
			if (conditionCount > 0) {
				str.append("AND ");
			}
			str.append("m.flags=? ");

			conditionCount++;
		}

		if (boardId != -1) {
			if (conditionCount > 0) {
				str.append("AND ");
			}
			// also get additional boards to handle cross posting
			str.append("(bm.board_id=? "
			+ "OR (bm.board_id<>? AND EXISTS "
			+ "(SELECT message_id FROM board_message inner_bm "
			+ "WHERE inner_bm.board_id=? AND bm.message_id = inner_bm.message_id))) ");

			conditionCount++;
		}


		if (conditionCount > 0) {
			str.append("AND ");
		}
		str.append("(rt.reply_order IS NULL OR rt.reply_order=0) ");
		if (recentCount > 0) {
			str.append(" ORDER BY m.message_id DESC LIMIT ?");
		}

		return str.toString();
	}

	public void setParameters(PreparedStatement pstmt) throws SQLException {
		int param = 0;

		if (identityId != -1) {
			pstmt.setInt(++param, identityId);
		} else if (from != null) {
			pstmt.setString(++param, toWildCard(from));
		}

		if (uuid != null) {
			pstmt.setString(++param, uuid);
		}

		if (subject != null) {
			pstmt.setString(++param, toWildCard(subject));
		}

		if (body != null) {
			pstmt.setString(++param, toWildCard(body));
		}

		if (flags > 0) {
			pstmt.setInt(++param, flags);
		}

		if (boardId != -1) {
			pstmt.setInt(++param, boardId);
			pstmt.setInt(++param, boardId);
			pstmt.setInt(++param, boardId);
		}

		if (recentCount > 0) {
			pstmt.setInt(++param, recentCount);
		}
	}

	private String toWildCard(String input) {
		StringBuilder str = new StringBuilder();
		str.append('%');
		str.append(input.replaceAll("(%|_|\\\\)", "\\\\$1"));
		str.append('%');

		return str.toString();
	}
}
