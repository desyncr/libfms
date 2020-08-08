package jfms.config;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class Constants {
	public static final String JFMS_CONFIG_PATH    = "jfms.properties";
	public static final String LOG_CONFIG_PATH     = "/logging.properties";
	public static final String PROPERTIES_PATH     = "/info.properties";
	public static final String AVATAR_DIR          = "cache";

	// timings
	public static final long STARTUP_IDLE_TIME       =  30; // 30s
	public static final int ONLINE_CHECK_INTERVAL    =  60; //  1m
	public static final int INSERT_IDLE_TIME         = 300; //  5m
	public static final long DOWNLOAD_IDLE_TIME      = 900; // 15m

	public static final int TTL_IDENTITY           =  120; //  2h
	public static final int TTL_TRUSTLIST          =  120; //  2h
	public static final int TTL_MESSAGELIST        =   60; //  1h
	public static final int TTL_MESSAGE            =  360; //  6h
	public static final int TTL_ID_INTRODUCTION    =   60; //  1h

	public static final int ADD_SEED_IDENTITY      =   -1;
	public static final int ADD_MANUALLY           =   -2;
	public static final int ADD_PUZZLE_SOLVED      =   -3;
	public static final int ADD_IMPORT             =   -4;
	public static final int ADD_UNKNOWN            =   -9;

	// FRED FetchException error codes are limited to 1024
	public static final int CODE_RECENTLY_TRIED    = 9001;
	public static final int CODE_PARSE_FAILED      = 9002;

	// default settings
	public static final String DEFAULT_FALSE       = "false";
	public static final String DEFAULT_TRUE        = "true";
	public static final String DEFAULT_FCP_HOST    = "127.0.0.1";
	public static final String DEFAULT_FCP_PORT    = "9481";
	public static final String DEFAULT_DEFAULT_ID  = "0";
	public static final String DEFAULT_MESSAGEBASE = "fms";
	public static final String DEFAULT_ICON_SET    = "oxygen";
	public static final String DEFAULT_MIN_LOCAL_TRUSTLIST_TRUST = "50";
	public static final String DEFAULT_MIN_LOCAL_MESSAGE_TRUST = "50";
	public static final String DEFAULT_MIN_PEER_TRUSTLIST_TRUST = "30";
	public static final String DEFAULT_MIN_PEER_MESSAGE_TRUST = "30";
	public static final String DEFAULT_INDIRECT_TRUST_WEIGHT = "50";
	public static final String DEFAULT_INACTIVITY_RETRY_INTERVAL = "3";
	public static final String DEFAULT_INACTIVITY_TIMEOUT = "7";
	public static final String DEFAULT_MAX_IDENTITY_AGE = "7";
	public static final String DEFAULT_MAX_MESSAGE_AGE = "30";
	public static final String DEFAULT_MAX_FCP_REQUESTS = "5";
	public static final String DEFAULT_DOWNLOAD_PRIORITY = "default";
	public static final String DEFAULT_UPLOAD_PRIORITY = "default";
	public static final String DEFAULT_LOG_LEVEL = "FINE";
	public static final String DEFAULT_WINDOW_SIZE = "1024x768";
	public static final int DEFAULT_WINDOW_WIDTH = 1024;
	public static final int DEFAULT_WINDOW_HEIGHT = 768;
	public static final int MIN_WINDOW_WIDTH = 640;
	public static final int MIN_WINDOW_HEIGHT = 480;
	public static final String DEFAULT_FOLDER_PANE_WIDTH =  "20";
	public static final String DEFAULT_HEADER_PANE_HEIGHT = "40";

	// fixed values
	public static final String DATABASE_FILE = "jfms.db3";
	public static final String DATABASE_URL = "jdbc:sqlite:" + DATABASE_FILE;
	public static final String DATABASE_DRIVER = "org.sqlite.JDBC";
	public static final String DATABASE_USER = null;
	public static final String DATABASE_PASSWORD = null;

	public static final int MSG_FLAG_STARRED = 0x01;

	public static final LocalDate FALLBACK_DATE = LocalDate.ofEpochDay(0);

	public static final int DEFAULT_SEED_TRUST = 90;
	public static final int MAX_INSERTS = 2;
	public static final int MAX_CONCURRENT_PUZZLE_REQUESTS = 2;
	public static final int MAX_PUZZLE_REQUESTS = 20;
	public static final int MAX_IDENTITY_INDEX = 0;
	public static final int MAX_TRUSTLIST_AGE = 15;
	public static final int MAX_TRUSTLIST_INDEX = 50;
	public static final int MAX_MESSAGELIST_INDEX = 50;
	public static final int MAX_FAST_MESSAGE_CHECK_COUNT = 20;
	public static final int MAX_SINGLE_USE_AGE = 7;
	public static final int MAX_INACTIVE_IDENTITY_REQUESTS = 50;
	public static final int MAX_PUZZLE_AGE = 7;
	public static final int MAX_INTRODUCTION_PUZZLE_INDEX = 50;
	public static final int MAX_INDEX = 9999;
	public static final int MAX_LOCAL_MESSAGE_AGE = 10;
	public static final int MAX_LOCAL_MESSAGELIST_COUNT = 50;
	public static final int MAX_MESSAGELIST_COUNT = 600;
	public static final int MAX_BOARD_LENGTH = 40;
	public static final int MAX_NAME_LENGTH = 40;
	public static final int MAX_SIGNATURE_LENGTH = 500;
	public static final int MAX_SUBJECT_LENGTH = 250;
	public static final int MAX_TRUST_COMMENT_LENGTH = 250;
	public static final int MAX_URI_LENGTH = 2048;
	public static final int MAX_REQUEST_SIZE = 1000000; // 1MB
	public static final int MAX_MESSAGE_DELAY  = 525600; // 1y

	public static final int MAX_CAPTCHA_WIDTH = 200;
	public static final int MAX_CAPTCHA_HEIGHT = 200;

	public static final String SEE_LOGS_TEXT =
		"Consult the log files for more information.";
	public static final String SHOW_THREADS_TEXT = "Group messages by thread";
	public static final String MONOSPACE_TEXT = "Use monospace font";
	public static final String EMOTICON_TEXT = "Show emoticons as graphics";
	public static final String MUTE_QUOTES_TEXT = "Mute quoted text";
	public static final String SHOW_SIGNATURE_TEXT = "Show signature";
	public static final String DETECT_LINKS_TEXT = "Detect links";
	public static final String SHOW_ATTACHMENTS_TEXT = "Show attachments";

	public static final List<String> getDefaultSeedIdentities() {
		return Arrays.asList(
				// SomeDude
				"SSK@NuBL7aaJ6Cn4fB7GXFb9Zfi8w1FhPyW3oKgU9TweZMw,iXez4j3qCpd596TxXiJgZyTq9o-CElEuJxm~jNNZAuA,AQACAAE/",
				// cptn_insano
				"SSK@bloE1LJ~qzSYUkU2nt7sB9kq060D4HTQC66pk5Q8NpA,DOOASUnp0kj6tOdhZJ-h5Tk7Ka50FSrUgsH7tCG1usU,AQACAAE/",
				// boardstat
				"SSK@aYWBb6zo2AM13XCNhsmmRKMANEx6PG~C15CWjdZziKA,X1pAG4EIqR1gAiyGFVZ1iiw-uTlh460~rFACJ7ZHQXk,AQACAAE/",
				// herb
				"SSK@5FeJUDg2ZdEqo-u4yoYWc1zF4tgPwOWlqcAJVGCoRv8,ptJ1y0YBkdU9S5DeYC8AsLH0SrmTE9S3w2HKZvl5QKo,AQACAAE/",
				// Tommy[D]
				"SSK@EefdujDZxdWxl0qusX0cJofGmJBvd3dF4Ty61PZy8Y8,4-LkBILohhpX7znBPXZWEUoK2qQZs-CLbUFO3-yKJIo,AQACAAE/",
				/// benjamin
				"SSK@y7xEHiGMGlivnCq-a8SpYU0YO-XRNI3LcJHB8tCeaXI,lRZOVc0pEHTEPqZUJqc5qRv6JDxHZzqc~ybEC~I2y~A,AQACAAE/",
				// Eye
				"SSK@vcQHxA8U6PxTbAxTAf65jc~sx4Tg3bWPf2ODLqR-SBg,P~Qf~geqmIk50ylnBav7OzmcFtmbr1YgNiuOuzge6Vc,AQACAAE/",
				// Mitosis
				"SSK@8~dscNP1TFUHWMZMZtpFJDrwg0rVePL6fB1S7uy4fTM,XWubHZK5Oizj0A9ovdob2wPeSmg4ikcduDMAAnvmkbw,AQACAAE/",
				// ZugaZandy
				"SSK@YoLiLuT0frl6DQb5b6Zz8CghW0ZC3P8xsBnEEE5puFE,6PiWr2ZGWqE5uSjEVJcNKz0NJF5xndr1TMRogR~RECQ,AQACAAE/",
				// Justus_Ranvier
				"SSK@JOKHnSe4cTWMCeQNSHr~-xqcYb2Tq0sVhDYPcklXhA8,p1bkPusgKdAD5pBdy3-ZvwgG-0WtBH4tIpgAYIu1oec,AQACAAE/"
			);
	}

	private Constants() {
	}
}
