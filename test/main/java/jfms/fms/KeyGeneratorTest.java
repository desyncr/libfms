package jfms.fms;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jfms.config.Config;
import jfms.config.Constants;

public class KeyGeneratorTest {
	@Before
	public void setUp() {
		Config.getInstance().setStringValue(Config.MESSAGEBASE,
				Constants.DEFAULT_MESSAGEBASE);
	}

	@Test
	public void testgetIdentityIntroductionKey() {
		LocalDate date = LocalDate.parse("2018-01-01",
				DateTimeFormatter.ISO_LOCAL_DATE);
		String uuid = "UUID";
		String solution = "fms12";

		String key = Identity.getIdentityIntroductionKey(date, uuid, solution);

		Assert.assertEquals("KSK@jfms.fms|2018-01-01|UUID|547F4D4ADD6D44A2E0A031965F7A0955F796D274.xml", key);
	}
}
