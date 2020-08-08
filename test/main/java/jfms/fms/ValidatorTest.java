package jfms.fms;

import org.junit.Assert;
import org.junit.Test;

public class ValidatorTest {
	@Test
	public void testValidateUuid() {
		final String u1 = "12345678-90AB-CDEF-1234-567890abcdef@abcdefghijklmnopqrstuvwxyz01234567890abcdef";
		Assert.assertTrue(Validator.isValidUuid(u1));

		final String u2 = "12345678-90AB-CDEF-1234-567890abcd\uC3A9f@abcdefghijklmnopqrstuvwxyz01234567890abcdef";
		Assert.assertFalse(Validator.isValidUuid(u2));
	}

	@Test
	public void testValidateUri() {
		final String u1 = "SSK@0npnMrqZNKRCRoGojZV93UNHCMN-6UU3rRSAmP6jNLE,~BG-edFtdCC1cSH4O3BWdeIYa8Sw5DfyrSV-TKdO5ec,AQACAAE/jfms.fms-147/activelink.png";
		Assert.assertTrue(Validator.isValidFreenetURI(u1));

		final String u2 = "USK@0iU87PXyodL2nm6kCpmYntsteViIbMwlJE~wlqIVvZ0,nenxGvjXDElX5RIZxMvwSnOtRzUKJYjoXEDgkhY6Ljw,AQACAAE/freenetproject-mirror/503/theme/images/logo-small.png";
		Assert.assertTrue(Validator.isValidFreenetURI(u2));

		final String u3 = "CHK@kYm75W6kIbuThOLZIqzUE3IYKZDO5QOQMEMBxzQ0bjQ,UxL-cw0vTOAvM1NXLxStWDW-8WnV-meUtTKImf6tQlA,AAMC--8";
		Assert.assertTrue(Validator.isValidFreenetURI(u3));

		final String u4 = "https://freenetproject.org/index.html";
		Assert.assertFalse(Validator.isValidFreenetURI(u4));
	}
}
