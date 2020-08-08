package jfms.fms;

import java.io.ByteArrayInputStream;
import java.time.LocalDate;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;
import jfms.fms.xml.IdentityIntroductionParser;
import jfms.store.Store;

public class IdentityIntroductionRequest extends DownloadRequest {
	private static final Logger LOG = Logger.getLogger(IdentityIntroductionRequest.class.getName());
	private static final IdentityIntroductionParser parser =
		new IdentityIntroductionParser();

	private final int localIdentityId;
	private final LocalDate date;
	private final int index;

	public IdentityIntroductionRequest(String id, int localIdentityId,
			LocalDate date, int index, String uuid, String solution) {

		super(id, Identity.getIdentityIntroductionKey(date, uuid, solution),
				Constants.TTL_ID_INTRODUCTION);
		this.localIdentityId = localIdentityId;
		this.date = date;
		this.index = index;
	}

	@Override
	public void finished(byte[] data) {
		Store store = FmsManager.getInstance().getStore();
		store.setPuzzleSolved(localIdentityId, date, index);

		String ssk = parser.parse(new ByteArrayInputStream(data));
		if (ssk != null) {
			FmsManager.getInstance().getIdentityManager()
				.addIdentityFromPuzzle(ssk);
		}
	}

	@Override
	public boolean redirect(String redirectURI) {
		LOG.log(Level.WARNING, "got unexpected redirect");

		return false;
	}
}
