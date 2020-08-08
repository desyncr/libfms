package jfms.fms.xml;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import jfms.fms.LocalIdentity;
import jfms.fms.Sanitizer;

public class IdentityExportWriter {
	private static final Logger LOG = Logger.getLogger(IdentityExportWriter.class.getName());

	public byte[] writeXml(List<LocalIdentity> identities) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		try {
			XMLStreamWriter xtw = Utils.createXMLStreamWriter(bos, "UTF-8");

			xtw.writeStartDocument("UTF-8", "1.0");
			xtw.writeStartElement("IdentityExport");

			for (LocalIdentity identity : identities) {
				xtw.writeStartElement("Identity");

				xtw.writeStartElement("Name");
				xtw.writeCharacters(Sanitizer.sanitizeName(identity.getName()));
				xtw.writeEndElement();

				xtw.writeStartElement("PublicKey");
				xtw.writeCharacters(identity.getSsk());
				xtw.writeEndElement();

				xtw.writeStartElement("PrivateKey");
				xtw.writeCharacters(identity.getPrivateSsk());
				xtw.writeEndElement();

				xtw.writeStartElement("SingleUse");
				xtw.writeCharacters(Boolean.toString(identity.getSingleUse()));
				xtw.writeEndElement();

				xtw.writeStartElement("PublishTrustList");
				xtw.writeCharacters(Boolean.toString(identity.getPublishTrustList()));
				xtw.writeEndElement();

				xtw.writeStartElement("PublishBoardList");
				xtw.writeCharacters(Boolean.toString(identity.getPublishBoardList()));
				xtw.writeEndElement();

				xtw.writeStartElement("PublishFreesite");
				xtw.writeCharacters(Boolean.toString(identity.getFreesiteEdition() != -1));
				xtw.writeEndElement();

				xtw.writeEndElement();
			}

			xtw.writeEndElement();
			xtw.writeEndDocument();

			xtw.flush();
			xtw.close();
		} catch (XMLStreamException e) {
			LOG.log(Level.WARNING, "Failed to create IdentityExport XML", e);
			bos.reset();
		}

		return bos.toByteArray();
	}
}
