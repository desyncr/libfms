package jfms.fms.xml;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import jfms.fms.Message;

public class MessageParserTest {
	@Test
	public void testParse() {
		MessageParser parser = new MessageParser();
		final String uuid = "12345678-90AB-CDEF-1234-567890abcdef@abcdefghijklmnopqrstuvwxyz01234567890abcdef";

		final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<Message>"
			+ "<Date>2018-05-01</Date>"
			+ "<Time>01:02:03</Time>"
			+ "<Subject>subject</Subject>"
			+ "<MessageID>" + uuid + "</MessageID>"
			+ "<ReplyBoard>test</ReplyBoard>"
			+ "<Body>lorem ipsum</Body>"
			+ "<Boards>"
			+ "<Board>test</Board>"
			+ "</Boards>"
			+ "</Message>";

		LocalDate expectedDate =
			LocalDate.parse("2018-05-01", DateTimeFormatter.ISO_LOCAL_DATE);
		LocalTime expectedTime =
			LocalTime.parse("01:02:03", DateTimeFormatter.ISO_LOCAL_TIME);

		Message msg = parser.parse(new ByteArrayInputStream(
					xml.getBytes(StandardCharsets.UTF_8)));
		Assert.assertNotNull(msg);
		Assert.assertEquals(expectedDate, msg.getDate());
		Assert.assertEquals(expectedTime, msg.getTime());
		Assert.assertEquals("subject", msg.getSubject());
		Assert.assertEquals(uuid, msg.getMessageUuid());
		Assert.assertEquals("test", msg.getReplyBoard());
		Assert.assertEquals("lorem ipsum", msg.getBody());
		Assert.assertEquals(1, msg.getBoards().size());
		Assert.assertEquals("test" , msg.getBoards().get(0));
	}

	@Test
	public void testSubSecondTime() {
		MessageParser parser = new MessageParser();
		final String uuid = "12345678-90AB-CDEF-1234-567890abcdef@abcdefghijklmnopqrstuvwxyz01234567890abcdef";

		final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<Message>"
			+ "<Date>2018-05-01</Date>"
			+ "<Time>01:02:03.123</Time>"
			+ "<Subject>subject</Subject>"
			+ "<MessageID>" + uuid + "</MessageID>"
			+ "<ReplyBoard>test</ReplyBoard>"
			+ "<Body>lorem ipsum</Body>"
			+ "<Boards>"
			+ "<Board>test</Board>"
			+ "</Boards>"
			+ "</Message>";

		LocalDate expectedDate =
			LocalDate.parse("2018-05-01", DateTimeFormatter.ISO_LOCAL_DATE);
		LocalTime expectedTime =
			LocalTime.parse("01:02:03", DateTimeFormatter.ISO_LOCAL_TIME);

		Message msg = parser.parse(new ByteArrayInputStream(
					xml.getBytes(StandardCharsets.UTF_8)));
		Assert.assertNotNull(msg);
		Assert.assertEquals(expectedDate, msg.getDate());
		Assert.assertEquals(expectedTime, msg.getTime());
		Assert.assertEquals("subject", msg.getSubject());
		Assert.assertEquals(uuid, msg.getMessageUuid());
		Assert.assertEquals("test", msg.getReplyBoard());
		Assert.assertEquals("lorem ipsum", msg.getBody());
		Assert.assertEquals(1, msg.getBoards().size());
		Assert.assertEquals("test" , msg.getBoards().get(0));
	}
}
