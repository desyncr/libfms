package jfms.fms.xml;

import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import jfms.fms.IntroductionPuzzle;

public class IntroductionPuzzleWriter {
	private static final Logger LOG = Logger.getLogger(IntroductionPuzzleWriter.class.getName());
	private final Base64.Encoder b64Enc = Base64.getEncoder();

	public byte[] writeXml(IntroductionPuzzle puzzle) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		try {
			XMLStreamWriter xtw = Utils.createXMLStreamWriter(bos, "UTF-8");

			xtw.writeStartDocument("UTF-8", "1.0");
			xtw.writeStartElement("IntroductionPuzzle");

			xtw.writeStartElement("Type");
			xtw.writeCharacters(puzzle.getType());
			xtw.writeEndElement();

			xtw.writeStartElement("UUID");
			xtw.writeCharacters(puzzle.getUuid());
			xtw.writeEndElement();

			xtw.writeStartElement("MimeType");
			xtw.writeCharacters(puzzle.getMimeType());
			xtw.writeEndElement();

			xtw.writeStartElement("PuzzleData");
			xtw.writeCharacters(b64Enc.encodeToString(puzzle.getData()));
			xtw.writeEndElement();

			xtw.writeEndElement();
			xtw.writeEndDocument();

			xtw.flush();
			xtw.close();
		} catch (XMLStreamException e) {
			LOG.log(Level.WARNING, "Failed to create Identity XML", e);
			bos.reset();
		}

		return bos.toByteArray();
	}
}
