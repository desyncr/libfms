package jfms.fms;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import jfms.config.Constants;
import jfms.fcp.FcpClient;
import jfms.fcp.FcpDirectoryEntry;
import jfms.fcp.FcpException;
import jfms.fcp.FcpListener;
import jfms.fms.xml.IdentityIntroductionWriter;
import jfms.fms.xml.IdentityWriter;
import jfms.fms.xml.IntroductionPuzzleWriter;
import jfms.fms.xml.MessageListWriter;
import jfms.fms.xml.MessageParser;
import jfms.fms.xml.TrustListWriter;
import jfms.store.InsertStatus;
import jfms.store.Store;
import jfms.util.RequestID;
import jfms.util.RequestLimiter;
import jfms.util.UUID;

public class InsertThread implements FcpListener, Runnable {
	public static final int FCP_ERROR_COLLISION = 9;
	private static final Logger LOG = Logger.getLogger(InsertThread.class.getName());

	private final FcpClient fcpClient;
	private final RequestID requestID = new RequestID("insert-");
	private RequestLimiter requestLimiter;
	private final Map<String, InsertRequest> requests = new ConcurrentHashMap<>();

	public InsertThread(FcpClient fcpClient) {
		this.fcpClient = fcpClient;
	}

	@Override
	public void run() {
		LOG.log(Level.FINE, "Started FMS insert thread with ID {0}",
				Thread.currentThread().getId());
		requestLimiter = new RequestLimiter();

		Store store = FmsManager.getInstance().getStore();

		try {
			fcpClient.start();

			LocalDate date = LocalDate.now(ZoneOffset.UTC);

			Map<Integer, LocalIdentity> identities = store.retrieveLocalIdentities();
			for (Map.Entry<Integer, LocalIdentity> e : identities.entrySet()) {

				final int localIdentityId = e.getKey();
				final LocalIdentity localIdentity = e.getValue();

				LOG.log(Level.FINEST, "Processing local identity {0}: {1}",
						new Object[]{e.getKey(),
						localIdentity.getFullName()});
				if (!localIdentity.getIsActive()) {
					LOG.log(Level.FINEST, "identity ist not active");
					continue;
				}

				if (localIdentity.getSingleUse()) {
					LocalDate lastActiveDay = localIdentity.getCreationDate()
						.plusDays(Constants.MAX_SINGLE_USE_AGE);
					if (date.isAfter(lastActiveDay)) {
						LOG.log(Level.FINEST, "single use identity expired");
						continue;
					}
				}

				requestLimiter.waitUntilReady(1);

				insertIdentity(localIdentityId, localIdentity, date);
				if (localIdentity.getPublishTrustList()) {
					insertTrustList(localIdentityId, localIdentity, date);
					insertIntroductionPuzzle(localIdentityId, localIdentity, date);
				}
				insertMessages(localIdentityId, localIdentity);
				insertMessageList(localIdentityId, localIdentity, date);

				insertIdentityIntroductions(localIdentityId, localIdentity);
			}
		} catch (InterruptedException e) {
			LOG.log(Level.FINE, "FMS insert thread interrupted");
		} catch (Exception e) {
			LOG.log(Level.WARNING, "exception in FMS insert thread", e);
		}

		LOG.log(Level.FINE, "FMS insert thread stopped");
	}

	@Override
	public void error(String fcpIdentifier, int code) {
		requestLimiter.requestDone();

		InsertRequest request = requests.remove(fcpIdentifier);
		if (request == null) {
			LOG.log(Level.WARNING, "got FCP response for unknown ID: {0}",
					fcpIdentifier);
			return;
		}

		if (request.getType() == null) {
			LOG.log(Level.WARNING, "request type not set");
			return;
		}

		Store store = FmsManager.getInstance().getStore();
		// TODO check if Fatal is set
		switch (request.getType()) {
		case IDENTITY:
			if (code == FCP_ERROR_COLLISION) {
				// Collided with existing data
				// assume insert was successful and set inserted flag in DB
				LOG.log(Level.INFO, "collison on identity insert "
						+ "(already inserted?)");
				store.updateInsert(RequestType.IDENTITY,
						request.getLocalIdentityId(),
						request.getLocalDate(),
						request.getIndex(),
						InsertStatus.IGNORE);
			}
			break;
		case TRUST_LIST:
			if (code == FCP_ERROR_COLLISION) {
				// Collided with existing data
				// increment index and retry
				LOG.log(Level.INFO, "collison on trust list insert, "
						+ "retrying with new index");
				store.incrementInsertIndex(RequestType.TRUST_LIST,
						request.getLocalIdentityId(),
						request.getLocalDate(),
						request.getIndex());
			}
			break;
		case MESSAGE:
			if (code == FCP_ERROR_COLLISION) {
				// Collided with existing data
				// increment index and retry
				LOG.log(Level.INFO, "collison on message insert, "
						+ "retrying with new index");
				store.incrementInsertIndex(RequestType.MESSAGE,
						request.getLocalIdentityId(),
						request.getLocalDate(),
						request.getIndex());
			}
			break;
		case IDENTITY_INTRODUCTION:
			LOG.log(Level.INFO, "insert of IdentityIntroduction failed "
				+ " (puzzle possibly solved by someone else");
			// do not bother with retries
			IdentityIntroduction idIntro =
				(IdentityIntroduction)request.getClientData();
			store.setIdentityIntroductionInserted(request.getLocalIdentityId(),
					request.getLocalDate(), idIntro.getUuid());
			break;
		}
	}

	@Override
	public void finished(String fcpIdentifier, byte[] data) {
		requestLimiter.requestDone();
	}

	@Override
	public void redirect(String fcpIdentifier, String redirectURI) {
		requestLimiter.requestDone();

		requests.remove(fcpIdentifier);
	}

	@Override
	public void putSuccessful(String fcpIdentifier, String key) {
		InsertRequest request = requests.remove(fcpIdentifier);
		if (request == null) {
			LOG.log(Level.WARNING, "got FCP response for unknown ID: {0}",
					fcpIdentifier);
			requestLimiter.requestDone();
			return;
		}

		Store store = FmsManager.getInstance().getStore();
		switch (request.getType()) {
		case IDENTITY:
			store.updateInsert(RequestType.IDENTITY,
					request.getLocalIdentityId(),
					request.getLocalDate(),
					request.getIndex(),
					InsertStatus.IGNORE);
			break;
		case TRUST_LIST:
			store.updateInsert(RequestType.TRUST_LIST,
					request.getLocalIdentityId(),
					request.getLocalDate(), request.getIndex(),
					InsertStatus.INSERTED);
			break;
		case MESSAGE:
			store.setLocalMessageInserted(request.getLocalIdentityId(),
					request.getLocalDate(),
					request.getIndex(),
					true);

			LocalDate date = LocalDate.now(ZoneOffset.UTC);
			int index = store.getInsertIndex(RequestType.MESSAGE_LIST,
					request.getLocalIdentityId(), date,
					InsertStatus.INSERTED);
			store.updateInsert(RequestType.MESSAGE_LIST,
					request.getLocalIdentityId(), date, index+1,
					InsertStatus.NOT_INSERTED);
			MessageReference msgRef = new MessageReference();
			msgRef.setDate(request.getLocalDate());
			msgRef.setIndex(request.getIndex());
			msgRef.setIdentityId(request.getLocalIdentityId());
			FmsManager.getInstance().getMessageManager()
					.addLocalMessage(msgRef, InsertStatus.INSERTED, InsertStatus.NOT_INSERTED);
			break;
		case MESSAGE_LIST:
			store.updateInsert(RequestType.MESSAGE_LIST,
					request.getLocalIdentityId(),
					request.getLocalDate(), request.getIndex(),
					InsertStatus.INSERTED);
			break;
		case INTRODUCTION_PUZZLE:
			IntroductionPuzzle puzzle = (IntroductionPuzzle)request.getClientData();
			store.saveIntroductionPuzzle(request.getLocalIdentityId(),
					request.getLocalDate(), request.getIndex(),
					puzzle);
			break;
		case IDENTITY_INTRODUCTION:
			IdentityIntroduction idIntro =
				(IdentityIntroduction)request.getClientData();
			store.setIdentityIntroductionInserted(request.getLocalIdentityId(),
					request.getLocalDate(), idIntro.getUuid());
			break;
		default:
			LOG.log(Level.WARNING, "unhandled type: {0}", request.getType());
		}

		requestLimiter.requestDone();
	}

	@Override
	public void keyPairGenerated(String fcpIdentifier, String publicKey,
			String privateKey) {
	}

	public void cancelMessage(int localIdentityId, LocalDate date, int index) {
		for (Map.Entry<String, InsertRequest> e : requests.entrySet()) {
			InsertRequest r = e.getValue();
			if (r.getType() == RequestType.MESSAGE &&
					r.getLocalIdentityId() == localIdentityId &&
					r.getLocalDate().equals(date) &&
					r.getIndex() == index) {

				String id = e.getKey();
				try {
					fcpClient.cancel(id);
				} catch (FcpException ex) {
					LOG.log(Level.FINE, "Failed to cancel request {0}", id);
					LOG.log(Level.FINE, "Failed with ", ex);
				}
				break;
			}
		}
	}

	private void insertIdentity(int localIdentityId,
			LocalIdentity localIdentity, LocalDate date)
			throws InterruptedException, FcpException {
		Store store = FmsManager.getInstance().getStore();

		IdentityWriter identityWriter = new IdentityWriter();

		int currentIndex = store.getInsertIndex(RequestType.IDENTITY,
				localIdentityId, date, InsertStatus.IGNORE);
		// currently, only insert identity once per day
		// -> changes will be effective on the next day
		if (currentIndex == -1) {
			requestLimiter.waitUntilReady(Constants.MAX_INSERTS);

			LOG.log(Level.FINEST, "Inserting identity");

			InsertRequest request = new InsertRequest(
					RequestType.IDENTITY,
					localIdentityId, date, 0);
			final String key = Identity.getIdentityKey(
					localIdentity.getPrivateSsk(), date, 0);
			byte[] identityXml = identityWriter.writeXml(localIdentity);

			if (identityXml.length != 0) {
				queueRequest(request, key, identityXml);
			}
		} else {
			LOG.log(Level.FINEST, "Skipping identity insert");
		}
	}

	private void insertTrustList(int localIdentityId,
			LocalIdentity localIdentity, LocalDate date)
		throws InterruptedException, FcpException {

		// currently, insert trust list once per day
		// TODO reference client inserts every 6 hours
		Store store = FmsManager.getInstance().getStore();
		int currentIndex = store.getInsertIndex(RequestType.TRUST_LIST,
				localIdentityId, date, InsertStatus.IGNORE);
		if (currentIndex != -1) {
			LOG.log(Level.FINEST, "Skipping trust list insert");
			return;
		}

		final LocalDate fromDate = date.minusDays(Constants.MAX_TRUSTLIST_AGE);
		final Set<String> recentSsks = store.getRecentSsks(fromDate);
		if (recentSsks.isEmpty()) {
			LOG.log(Level.INFO,
					"No recently seen identities, skip trust list insert");
			return;
		}

		LOG.log(Level.FINEST, "found {0} recently seen identities",
				recentSsks.size());

		final Map<String, Trust> localTrusts =
			store.getLocalTrustList(localIdentityId).stream()
			.collect(Collectors.toMap(Trust::getIdentity, Function.identity()));
		LOG.log(Level.FINEST, "found {0} entries in local trustlist",
				localTrusts.size());

		List<Trust> trustList = new ArrayList<>(recentSsks.size());

		for (String ssk : recentSsks) {
			Trust localTrust = localTrusts.get(ssk);
			if (localTrust != null) {
				trustList.add(localTrust);
			} else {
				trustList.add(new Trust(ssk));
			}
		}


		TrustListWriter trustListWriter = new TrustListWriter();
		byte[] trustListXml = trustListWriter.writeXml(trustList);
		if (trustListXml.length == 0) {
			return;
		}

		int index = 0;
		InsertRequest request = new InsertRequest(
				RequestType.TRUST_LIST,
				localIdentityId, date, index);
		final String key = Identity.getTrustListKey(
				localIdentity.getPrivateSsk(), date, index);

		queueRequest(request, key, trustListXml);
	}

	private void insertIntroductionPuzzle(int localIdentityId,
			LocalIdentity localIdentity, LocalDate date)
		throws InterruptedException, FcpException {

	}

	private void insertMessages(int localIdentityId,
			LocalIdentity localIdentity)
			throws InterruptedException, FcpException {
		Store store = FmsManager.getInstance().getStore();
		final List<MessageReference> localMessages =
			store.getLocalMessageList(localIdentityId,
					InsertStatus.NOT_INSERTED, null, -1);

		LOG.log(Level.FINEST, "found {0} local messages for insert",
				localMessages.size());
		int messageCount = 0;
		for (MessageReference m : localMessages) {
			requestLimiter.waitUntilReady(Constants.MAX_INSERTS);

			InsertRequest request = new InsertRequest(
					RequestType.MESSAGE,
					localIdentityId, m.getDate(), m.getIndex());

			final String key = Identity.getMessageKey(
					localIdentity.getPrivateSsk(), m.getDate(), m.getIndex());
			final String messageXml = store.getLocalMessage(localIdentityId,
					m.getDate(), m.getIndex());
			if (messageXml == null) {
				continue;
			}
			final byte[] data = messageXml.getBytes(StandardCharsets.UTF_8);

			MessageParser parser = new MessageParser();
			Message parsedMessage = parser.parse(new ByteArrayInputStream(data));
			if (parsedMessage != null) {
				LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
				LocalDateTime msgTime = LocalDateTime.of(
						parsedMessage.getDate(), parsedMessage.getTime());
				if (now.isAfter(msgTime)) {
					queueRequest(request, key, data);
					messageCount++;
				} else {
					LOG.log(Level.FINE, "Skipping delayed message");
				}
			} else {
				LOG.log(Level.INFO, "Skipping invalid message");
			}
		}

		// wait for message inserts to finish before proceeding with
		// message list insert
		if (messageCount > 0) {
			requestLimiter.waitUntilReady(1);
		}
	}

	private void insertMessageList(int localIdentityId,
			LocalIdentity localIdentity, LocalDate date)
			throws InterruptedException, FcpException {

		Store store = FmsManager.getInstance().getStore();
		// check for a message list that has not been inserted yet
		int index = store.getInsertIndex(RequestType.MESSAGE_LIST,
				localIdentityId, date, InsertStatus.NOT_INSERTED);
		if (index == -1) {
			// check if we have already inserted message list for today
			if (store.getInsertIndex(RequestType.MESSAGE_LIST,
					localIdentityId, date, InsertStatus.IGNORE) == -1) {
				// insert with index 0
				index = 0;
			}
		}

		if (index < 0) {
			LOG.log(Level.FINEST, "Skipping message list insert");
			return;
		}

		final int maxMessages = Constants.MAX_MESSAGELIST_COUNT;

		// XXX should we jfms.store boards to avoid parsing of XML files?
		LocalDate fromDate = date.minusDays(Constants.MAX_LOCAL_MESSAGE_AGE);
		final List<MessageReference> localMessages =
			store.getLocalMessageList(localIdentityId, InsertStatus.INSERTED,
					fromDate , -1);
		LOG.log(Level.FINEST, "found {0} local messages", localMessages.size());
		Iterator<MessageReference> iter = localMessages.iterator();
		while (iter.hasNext()) {
			MessageReference m = iter.next();
			requestLimiter.waitUntilReady(Constants.MAX_INSERTS);

			String xmlMessage = store.getLocalMessage(localIdentityId,
					m.getDate(), m.getIndex());
			if (xmlMessage == null) {
				iter.remove();
				continue;
			}

			MessageParser parser = new MessageParser();
			Message msg = parser.parse(new StringReader(xmlMessage));
			if (msg != null) {
				m.setBoards(msg.getBoards());
			} else {
				iter.remove();
			}
		}

		final int maxExternalMessages = maxMessages - localMessages.size();
		List<MessageReference> externalMessageList =
			store.getExternalMessageList(localIdentityId, maxExternalMessages);

		if (localMessages.size() + externalMessageList.size() == 0) {
			LOG.log(Level.FINE, "No messages found, skipping MessageList insert");
			return;
		}

		// XXX necessary to jfms.store index?
		MessageListWriter messageListWriter = new MessageListWriter();
		byte[] messageListXml = messageListWriter.writeXml(localMessages, externalMessageList);
		if (messageListXml.length == 0) {
			return;
		}

		InsertRequest request = new InsertRequest(
				RequestType.MESSAGE_LIST,
				localIdentityId, date, index);
		final String key = Identity.getMessageListKey(
				localIdentity.getPrivateSsk(), date, index, false);

		final String id = requestID.getNext();
		requests.put(id, request);
		fcpClient.insertDirectory(id, key,
				new FcpDirectoryEntry[] {
					new FcpDirectoryEntry("MessageList.xml", messageListXml)
				},
				"MessageList.xml", this,
				Arrays.asList("IgnoreUSKDatehints=true"));

		requestLimiter.addRequest();
	}

	private void insertIdentityIntroductions(int localIdentityId,
			LocalIdentity localIdentity)
		throws InterruptedException, FcpException {

		Store store = FmsManager.getInstance().getStore();
		final List<IdentityIntroduction> idIntros =
			store.getIdentityIntroductions(localIdentityId,
					InsertStatus.NOT_INSERTED);

		LOG.log(Level.FINEST, "found {0} IdentityIntroductions for insert",
				idIntros.size());
		for (IdentityIntroduction idIntro : idIntros) {
			requestLimiter.waitUntilReady(Constants.MAX_INSERTS);

			IdentityIntroductionWriter xmlWriter = new IdentityIntroductionWriter();
			byte[] xml = xmlWriter.writeXml(localIdentity.getSsk());
			if (xml.length == 0) {
				continue;
			}

			InsertRequest request = new InsertRequest(
					RequestType.IDENTITY_INTRODUCTION,
					localIdentityId, idIntro.getDate(), -1);
			request.setClientData(idIntro);
			final String key = Identity.getIdentityIntroductionKey(
					idIntro.getDate(), idIntro.getUuid(), idIntro.getSolution());

			queueRequest(request, key, xml);
		}
	}

	private void queueRequest(InsertRequest request, String key, byte[] data) throws FcpException {
		final String id = requestID.getNext();
		requests.put(id, request);
		fcpClient.insertKey(id, key, data, this);

		requestLimiter.addRequest();
	}
}
