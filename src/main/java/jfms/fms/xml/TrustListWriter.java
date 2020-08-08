package jfms.fms.xml;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import jfms.fms.Trust;

public class TrustListWriter {
	private static final Logger LOG = Logger.getLogger(TrustListWriter.class.getName());

	public byte[] writeXml(List<Trust> trustList) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		try {
			XMLStreamWriter xtw = Utils.createXMLStreamWriter(bos, "UTF-8");

			xtw.writeStartDocument("UTF-8", "1.0");
			xtw.writeStartElement("TrustList");
			xtw.writeAttribute("Version", "1.0");

			for (Trust trust : trustList) {
				xtw.writeStartElement("Trust");

				xtw.writeStartElement("Identity");
				xtw.writeCharacters(trust.getIdentity());
				xtw.writeEndElement();

				if (trust.getMessageTrustLevel() >= 0) {
					xtw.writeStartElement("MessageTrustLevel");
					xtw.writeCharacters(
							Integer.toString(trust.getMessageTrustLevel()));
					xtw.writeEndElement();
				}

				if (trust.getTrustListTrustLevel() >= 0) {
					xtw.writeStartElement("TrustListTrustLevel");
					xtw.writeCharacters(
							Integer.toString(trust.getTrustListTrustLevel()));
					xtw.writeEndElement();
				}

				if (trust.getMessageTrustComment() != null) {
					xtw.writeStartElement("MessageTrustComment");
					xtw.writeCharacters(trust.getMessageTrustComment());
					xtw.writeEndElement();
				}

				if (trust.getTrustListTrustComment() != null) {
					xtw.writeStartElement("TrustListTrustComment");
					xtw.writeCharacters(trust.getTrustListTrustComment());
					xtw.writeEndElement();
				}

				xtw.writeStartElement("IsFMS");
				xtw.writeCharacters("true");
				xtw.writeEndElement();

				xtw.writeEndElement();
			}

			xtw.writeEndElement();
			xtw.writeEndDocument();

			xtw.flush();
			xtw.close();
		} catch (XMLStreamException e) {
			LOG.log(Level.WARNING, "Failed to create TrustList", e);
			bos.reset();
		}

		return bos.toByteArray();
	}
}
