package jfms.fms.xml;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

import jfms.fms.IntroductionPuzzle;

public class IntroductionPuzzleParserTest {
	@Test
	public void testParse() {
		IntroductionPuzzleParser parser = new IntroductionPuzzleParser();
		final String uuid = "12345678-90AB-CDEF-1234-567890abcdef@abcdefghijklmnopqrstuvwxyz01234567890abcdef";

		final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "<IntroductionPuzzle>"
			+ "<Type>captcha</Type>"
			+ "<UUID>" + uuid + "</UUID>"
			+ "<MimeType>image/bmp</MimeType>"
			+ "<PuzzleData>cHV6emxlMQ==</PuzzleData>"
			+ "</IntroductionPuzzle>";

		IntroductionPuzzle puzzle = parser.parse(new ByteArrayInputStream(
					xml.getBytes(StandardCharsets.UTF_8)));
		Assert.assertNotNull(puzzle);
		Assert.assertEquals("captcha", puzzle.getType());
		Assert.assertEquals(uuid, puzzle.getUuid());
		Assert.assertEquals("image/bmp", puzzle.getMimeType());
		Assert.assertArrayEquals("puzzle1".getBytes(StandardCharsets.UTF_8),
				puzzle.getData());
	}
}
