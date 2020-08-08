package jfms.fms.xml;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

import jfms.fms.Identity;

public class IdentityParserTest {
	@Test
	public void testXXE() {
		IdentityParser parser = new IdentityParser();

		final String xml = "<!DOCTYPE foo [ <!ENTITY xxe SYSTEM \"file:///etc/os-release\" > ]>"
			+ "<Identity><Name>&xxe;</Name></Identity>";

		Identity id = parser.parse(new ByteArrayInputStream(
					xml.getBytes(StandardCharsets.UTF_8)));
		Assert.assertNull(id);
	}
}
