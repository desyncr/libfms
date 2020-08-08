package jfms.fms.xml;

import java.io.ByteArrayOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;


public class IdentityIntroductionWriter {
	private static final Logger LOG = Logger.getLogger(IdentityIntroductionWriter.class.getName());

	public byte[] writeXml(String ssk) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		try {
			XMLStreamWriter xtw = Utils.createXMLStreamWriter(bos, "UTF-8");

			xtw.writeStartDocument("UTF-8", "1.0");
			xtw.writeStartElement("IdentityIntroduction");

			xtw.writeStartElement("Identity");
			xtw.writeCharacters(ssk);
			xtw.writeEndElement();

			xtw.writeEndElement();
			xtw.writeEndDocument();

			xtw.flush();
			xtw.close();
		} catch (XMLStreamException e) {
			LOG.log(Level.WARNING, "Failed to create IdentityIntroduction XML", e);
			bos.reset();
		}

		return bos.toByteArray();
	}
}
