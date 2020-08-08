package jfms.fms.xml;

import java.io.InputStream;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import jfms.fms.IntroductionPuzzle;
import jfms.fms.Validator;

public class IntroductionPuzzleParser {
	private static final Logger LOG = Logger.getLogger(IntroductionPuzzleParser.class.getName());

	public IntroductionPuzzle parse(InputStream is) {
		IntroductionPuzzle puzzle = null;

		try {
			XMLStreamReader reader = Utils.createXMLStreamReader(is);

			while (reader.hasNext()) {
				if (reader.next() == XMLStreamConstants.START_ELEMENT) {
					if (reader.getLocalName().equals("IntroductionPuzzle")) {
						puzzle = parseIntroductionPuzzle(reader);
					} else {
						LOG.log(Level.INFO, "IntroductionPuzzle element not found");
					}
					break;
				}
			}
		} catch (XMLStreamException e) {
			LOG.log(Level.WARNING, "Failed to parse introduction puzzle", e);
		}

		return puzzle;
	}

	private IntroductionPuzzle parseIntroductionPuzzle(XMLStreamReader reader)
	throws XMLStreamException {

		int level = 1;
		IntroductionPuzzle puzzle = new IntroductionPuzzle();

		do {
			int event = reader.next();
			switch(event) {
			case XMLStreamConstants.START_ELEMENT:
				level++;
				if (level == 2) {
					switch (reader.getLocalName()) {
					case "Type":
						puzzle.setType(reader.getElementText());
						level--;
						break;
					case "UUID":
						final String uuid = reader.getElementText();
						if (Validator.isValidUuid(uuid)) {
							puzzle.setUUID(uuid);
						} else {
							LOG.log(Level.WARNING, "invalid UUID in IntroductionPuzzle");
						}
						level--;
						break;
					case "MimeType":
						puzzle.setMimeType(reader.getElementText());
						level--;
						break;
					case "PuzzleData":
						final String data = reader.getElementText();
						try {
							Base64.Decoder b64Dec = Base64.getDecoder();
							final byte[] decoded = b64Dec.decode(data);
							puzzle.setData(decoded);
						} catch (IllegalArgumentException e) {
							LOG.log(Level.INFO, "Failed to parse PuzzleData", e);
						}
						level--;
						break;
					}
				}
				break;
			case XMLStreamConstants.END_ELEMENT:
				level--;
				break;
			}
		} while (level > 0);

		boolean valid =
			puzzle.getType() != null &&
			puzzle.getUuid() != null &&
			puzzle.getMimeType() != null &&
			puzzle.getData() != null;

		if (!valid) {
			LOG.log(Level.WARNING, "skipping invalid IntroductionPuzzle");
			return null;
		}

		return puzzle;
	}
}
