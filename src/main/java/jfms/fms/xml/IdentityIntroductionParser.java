package jfms.fms.xml;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import jfms.fms.Validator;

public class IdentityIntroductionParser {
	private static final Logger LOG = Logger.getLogger(IdentityIntroductionParser.class.getName());

	public String parse(InputStream is) {
		String ssk = null;

		try {
			XMLStreamReader reader = Utils.createXMLStreamReader(is);

			while (reader.hasNext()) {
				if (reader.next() == XMLStreamConstants.START_ELEMENT) {
					if (reader.getLocalName().equals("IdentityIntroduction")) {
						ssk = parseIdentityIntroduction(reader);
					} else {
						LOG.log(Level.INFO, "IdentityIntroduction element not found");
					}
					break;
				}
			}
		} catch (XMLStreamException e) {
			LOG.log(Level.WARNING, "Failed to parse identity introduction", e);
		}

		return ssk;
	}

	private String parseIdentityIntroduction(XMLStreamReader reader) throws XMLStreamException {
		int level = 1;
		String ssk = null;

		do {
			int event = reader.next();
			switch(event) {
			case XMLStreamConstants.START_ELEMENT:
				level++;
				if (level == 2 && reader.getLocalName().equals("Identity")) {
					String value = reader.getElementText();
					if (Validator.isValidSsk(value)) {
						ssk = value;
					} else {
						LOG.log(Level.WARNING, "invalid SSK in identity introduction");
					}
					level--;
				}
				break;
			case XMLStreamConstants.END_ELEMENT:
				level--;
				break;
			}
		} while (level > 0);

		return ssk;
	}
}
