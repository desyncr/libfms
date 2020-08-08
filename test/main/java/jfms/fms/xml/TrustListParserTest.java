package jfms.fms.xml;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import jfms.fms.Trust;

public class TrustListParserTest {
	@Test
	public void testParse() {
		TrustListParser parser = new TrustListParser();
		final String ssk = "SSK@NuBL7aaJ6Cn4fB7GXFb9Zfi8w1FhPyW3oKgU9TweZMw,iXez4j3qCpd596TxXiJgZyTq9o-CElEuJxm~jNNZAuA,AQACAAE/";

		final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<TrustList Version=\"1.0\">"
			+ "<Trust>"
			+ "<Identity>" + ssk + "</Identity>"
			+ "<IsFMS>true</IsFMS>"
			+ "</Trust>"
			+ "</TrustList>";

		List<Trust> trustList = parser.parse(new ByteArrayInputStream(
					xml.getBytes(StandardCharsets.UTF_8)));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
	}

	@Test
	public void testVersionMissing() {
		TrustListParser parser = new TrustListParser();
		final String ssk = "SSK@NuBL7aaJ6Cn4fB7GXFb9Zfi8w1FhPyW3oKgU9TweZMw,iXez4j3qCpd596TxXiJgZyTq9o-CElEuJxm~jNNZAuA,AQACAAE/";

		final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<TrustList>"
			+ "<Trust>"
			+ "<Identity>" + ssk + "</Identity>"
			+ "<IsFMS>true</IsFMS>"
			+ "</Trust>"
			+ "</TrustList>";

		List<Trust> trustList = parser.parse(new ByteArrayInputStream(
					xml.getBytes(StandardCharsets.UTF_8)));
		Assert.assertNotNull(trustList);
		Assert.assertTrue(trustList.isEmpty());
	}

	@Test
	public void testDuplicateEntry() {
		TrustListParser parser = new TrustListParser();
		final String ssk = "SSK@NuBL7aaJ6Cn4fB7GXFb9Zfi8w1FhPyW3oKgU9TweZMw,iXez4j3qCpd596TxXiJgZyTq9o-CElEuJxm~jNNZAuA,AQACAAE/";

		final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<TrustList Version=\"1.0\">"
			+ "<Trust>"
			+ "<Identity>" + ssk + "</Identity>"
			+ "<IsFMS>true</IsFMS>"
			+ "</Trust>"
			+ "<Trust>"
			+ "<Identity>" + ssk + "</Identity>"
			+ "<IsFMS>true</IsFMS>"
			+ "</Trust>"
			+ "</TrustList>";

		List<Trust> trustList = parser.parse(new ByteArrayInputStream(
					xml.getBytes(StandardCharsets.UTF_8)));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
		Assert.assertEquals(ssk, trustList.get(0).getIdentity());
	}

	@Test
	public void testTrustLevel() {
		TrustListParser parser = new TrustListParser();
		List<Trust> trustList;
		Trust trust;

		trustList = parser.parse(createXmlWithTrustLevel("0"));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
		Assert.assertEquals(0, trustList.get(0).getMessageTrustLevel());

		trustList = parser.parse(createXmlWithTrustLevel("100"));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
		Assert.assertEquals(100, trustList.get(0).getMessageTrustLevel());

		trustList = parser.parse(createXmlWithTrustLevel("-1"));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
		Assert.assertEquals(-1, trustList.get(0).getMessageTrustLevel());

		trustList = parser.parse(createXmlWithTrustLevel("-2"));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
		Assert.assertEquals(-1, trustList.get(0).getMessageTrustLevel());

		trustList = parser.parse(createXmlWithTrustLevel("101"));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
		Assert.assertEquals(-1, trustList.get(0).getMessageTrustLevel());

		trustList = parser.parse(createXmlWithTrustLevel(""));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
		Assert.assertEquals(-1, trustList.get(0).getMessageTrustLevel());

		trustList = parser.parse(createXmlWithTrustLevel("x"));
		Assert.assertNotNull(trustList);
		Assert.assertEquals(1, trustList.size());
		Assert.assertEquals(-1, trustList.get(0).getMessageTrustLevel());
	}

	private InputStream createXmlWithTrustLevel(String level) {
		final String ssk = "SSK@NuBL7aaJ6Cn4fB7GXFb9Zfi8w1FhPyW3oKgU9TweZMw,iXez4j3qCpd596TxXiJgZyTq9o-CElEuJxm~jNNZAuA,AQACAAE/";
		final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<TrustList Version=\"1.0\">"
			+ "<Trust>"
			+ "<Identity>" + ssk + "</Identity>"
			+ "<MessageTrustLevel>" + level + "</MessageTrustLevel>"
			+ "<IsFMS>true</IsFMS>"
			+ "</Trust>"
			+ "</TrustList>";

		return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
	}
}
