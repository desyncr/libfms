package jfms.fms;

import java.io.ByteArrayInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.fms.xml.IntroductionPuzzleParser;
import jfms.util.UUID;

public class IntroductionPuzzleRequest extends DownloadRequest {
	private static final Logger LOG = Logger.getLogger(IntroductionPuzzleRequest.class.getName());

	private final String ssk;
	private final RequestTracker requestTracker;
	private IntroductionPuzzle selectedPuzzle;


	public IntroductionPuzzleRequest(String id, String ssk,
			RequestTracker requestTracker) {

		super(id, Identity.getIntroductionPuzzleKey(ssk,
					requestTracker.getDate(), requestTracker.getIndex()));
		this.ssk = ssk;
		this.requestTracker = requestTracker;
	}

	@Override
	public void finished(byte[] data) {
		IntroductionPuzzle puzzle = parsePuzzle(data);
		if (puzzle != null) {
			LOG.log(Level.FINEST, "Found puzzle with type {0}",
					puzzle.getType());
			selectedPuzzle = puzzle;
		}

		final RequestTracker nextTracker = requestTracker.incrementIndex();
		if (nextTracker != null) {
			LOG.log(Level.FINEST, "Got puzzle. Adding request for next puzzle");

			IntroductionPuzzleRequest nextRequest =
				new IntroductionPuzzleRequest(getNextId(), ssk, nextTracker);
			nextRequest.setPuzzle(selectedPuzzle);
			setChainedRequest(nextRequest);
		}
	}

	public IntroductionPuzzle getPuzzle() {
		return selectedPuzzle;
	}

	private void setPuzzle(IntroductionPuzzle puzzle) {
		selectedPuzzle = puzzle;
	}

	public String getSsk() {
		return ssk;
	}

	private IntroductionPuzzle parsePuzzle(byte[] data) {
		IntroductionPuzzleParser parser = new IntroductionPuzzleParser();
		IntroductionPuzzle puzzle =
			parser.parse(new ByteArrayInputStream(data));
		if (puzzle == null) {
			return null;
		}

		if (!"captcha".equals(puzzle.getType())) {
			LOG.log(Level.FINE, "unsupported CAPTCHA type {0}",
					puzzle.getType());
			return null;
		}

		if (FmsManager.getInstance().getStore().
				isIntroductionPuzzledSolved(puzzle.getUuid())) {
			LOG.log(Level.FINE, "puzzle already solved",
					puzzle.getType());
			return null;
		}

		final String uuid = puzzle.getUuid();
		if (!UUID.check(ssk, uuid)) {
			LOG.log(Level.INFO, "wrong puzzle UUID {0} ", uuid);
			return null;
		}

		return puzzle;
	}
}
