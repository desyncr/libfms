package jfms.fms;

import java.io.File;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jfms.store.InsertStatus;
import jfms.store.Store;
import jfms.store.Utils;

public class StoreLocalMessageTest {
	private static final String TEST_DB = "test.db3";
	private Store store;

	public static LocalDate date(String dateStr) {
		return LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
	}

	@Before
	public void setUp() throws SQLException {
		new File(TEST_DB).delete();

		store = new Store("jdbc:sqlite:" + TEST_DB);
		store.initialize(null);
	}

	@Test
	public void testGetLocalMessageList() throws SQLException {
		LocalDate fromDate = Utils.date("2018-02-10");
		final String ssk = "SSK@1234567890123456789012345678901234567890123,1234567890123456789012345678901234567890123,1234567/";


		LocalIdentity id = new LocalIdentity();
		id.setSsk(ssk);
		id.setPrivateSsk(ssk);
		int id1 = store.saveLocalIdentity(id);

		List<MessageReference> messageList;
		messageList = store.getLocalMessageList(id1, InsertStatus.NOT_INSERTED,
				fromDate, -1);
		Assert.assertTrue(messageList.isEmpty());
		messageList = store.getLocalMessageList(id1, InsertStatus.INSERTED,
				fromDate, -1);
		Assert.assertTrue(messageList.isEmpty());

		// add local message (10 days old) with index 0
		LocalDate msgDate1 = Utils.date("2018-02-10");

		store.saveLocalMessage(id1, "<msg1/>", msgDate1, null, InsertStatus.NOT_INSERTED);
		messageList = store.getLocalMessageList(id1, InsertStatus.NOT_INSERTED,
				fromDate, -1);
		Assert.assertEquals(1, messageList.size());
		messageList = store.getLocalMessageList(id1, InsertStatus.INSERTED,
				fromDate, -1);
		Assert.assertTrue(messageList.isEmpty());

		store.setLocalMessageInserted(id1, msgDate1, 0, true);
		messageList = store.getLocalMessageList(id1, InsertStatus.NOT_INSERTED,
				fromDate, -1);
		Assert.assertTrue(messageList.isEmpty());
		messageList = store.getLocalMessageList(id1, InsertStatus.INSERTED,
				fromDate, -1);
		Assert.assertEquals(1, messageList.size());

		MessageReference msgRef;
		msgRef = messageList.get(0);
		Assert.assertEquals(msgDate1, msgRef.getDate());
		Assert.assertEquals(0, msgRef.getIndex());


		// add local message (11 days old) with index 0
		LocalDate msgDate2 = Utils.date("2018-02-09");

		store.saveLocalMessage(id1, "<msg2/>", msgDate2, null, InsertStatus.NOT_INSERTED);
		messageList = store.getLocalMessageList(id1, InsertStatus.NOT_INSERTED,
				fromDate, -1);
		Assert.assertTrue(messageList.isEmpty());
		messageList = store.getLocalMessageList(id1, InsertStatus.INSERTED,
				fromDate, -1);
		Assert.assertEquals(1, messageList.size());

		store.setLocalMessageInserted(id1, msgDate2, 0, true);
		messageList = store.getLocalMessageList(id1, InsertStatus.NOT_INSERTED,
				fromDate, -1);
		Assert.assertTrue(messageList.isEmpty());
		messageList = store.getLocalMessageList(id1, InsertStatus.INSERTED,
				fromDate, -1);
		msgRef = messageList.get(0);
		Assert.assertEquals(msgDate1, msgRef.getDate());
		Assert.assertEquals(0, msgRef.getIndex());
	}

	@Test
	public void testStoreDraft() throws SQLException {
		final String ssk  = "SSK@1234567890123456789012345678901234567890123,1234567890123456789012345678901234567890123,1234567/";
		final String ssk2 = "SSK@2222222222123456789012345678901234567890123,1234567890123456789012345678901234567890123,1234567/";


		LocalIdentity id = new LocalIdentity();
		id.setSsk(ssk);
		id.setPrivateSsk(ssk);
		int id1 = store.saveLocalIdentity(id);

		id = new LocalIdentity();
		id.setSsk(ssk2);
		id.setPrivateSsk(ssk2);
		int id2 = store.saveLocalIdentity(id);

		List<MessageReference> messageList;
		messageList = store.getLocalMessageList(id1, InsertStatus.DRAFT,
				null, -1);
		Assert.assertTrue(messageList.isEmpty());

		// save message as draft
		LocalDate msgDate1 = Utils.date("2018-02-10");
		int msgId1 = store.saveLocalMessage(id1, "<msg1/>", msgDate1, null, InsertStatus.DRAFT);
		Assert.assertEquals(0, msgId1);

		messageList = store.getLocalMessageList(id1, InsertStatus.DRAFT, null, -1);
		Assert.assertEquals(1, messageList.size());
		Assert.assertEquals("<msg1/>", store.getLocalMessage(id1, msgDate1, msgId1));

		// edit message and save as draft
		MessageReference draftRef = new MessageReference();
		draftRef.setIdentityId(id1);
		draftRef.setDate(msgDate1);
		draftRef.setIndex(msgId1);

		msgId1 = store.saveLocalMessage(id1, "<msg1_v2/>", msgDate1, draftRef, InsertStatus.DRAFT);
		Assert.assertEquals(0, msgId1);
		Assert.assertEquals("<msg1_v2/>", store.getLocalMessage(id1, msgDate1, msgId1));

		// edit message and move to outbox
		msgId1 = store.saveLocalMessage(id1, "<msg1_v3/>", msgDate1, draftRef, InsertStatus.NOT_INSERTED);
		Assert.assertEquals(0, msgId1);
		Assert.assertEquals("<msg1_v3/>", store.getLocalMessage(id1, msgDate1, msgId1));

		// save 2nd message as draft
		int msgId2 = store.saveLocalMessage(id1, "<msg2/>", msgDate1, null, InsertStatus.DRAFT);
		Assert.assertEquals(1, msgId2);
		Assert.assertEquals("<msg2/>", store.getLocalMessage(id1, msgDate1, msgId2));

		// move 2nd message to outbox with different identity
		draftRef.setIndex(msgId2);

		messageList = store.getLocalMessageList(id2, InsertStatus.IGNORE, null, -1);
		Assert.assertTrue(messageList.isEmpty());

		int savedMsgId = msgId2;
		msgId2 = store.saveLocalMessage(id2, "<msg2_v2/>", msgDate1, draftRef, InsertStatus.NOT_INSERTED);
		Assert.assertEquals(0, msgId2);
		Assert.assertEquals("<msg2_v2/>", store.getLocalMessage(id2, msgDate1, msgId2));
		Assert.assertNull(store.getLocalMessage(id1, msgDate1, savedMsgId));

		// add 3rd (old) message as draft
		LocalDate msgDate2 = Utils.date("2018-01-01");
		int msgId3 = store.saveLocalMessage(id1, "<msg3/>", msgDate2, null, InsertStatus.DRAFT);
		Assert.assertEquals(0, msgId3);
		Assert.assertEquals("<msg3/>", store.getLocalMessage(id1, msgDate2, msgId3));

		// edit 3rd (old) message some days later
		LocalDate msgDate3 = Utils.date("2018-01-02");
		draftRef.setDate(msgDate2);
		draftRef.setIndex(msgId3);
		msgId3 = store.saveLocalMessage(id1, "<msg3_v2/>", msgDate3, draftRef, InsertStatus.DRAFT);
		// the message should now be stored with the new date
		Assert.assertEquals(0, msgId3);
		Assert.assertNull(store.getLocalMessage(id1, msgDate2, msgId3));
		Assert.assertEquals("<msg3_v2/>", store.getLocalMessage(id1, msgDate3, msgId3));

		// edit 3rd (old) message again
		draftRef.setDate(msgDate3);
		draftRef.setIndex(msgId3);
		msgId3 = store.saveLocalMessage(id1, "<msg3_v3/>", msgDate1, draftRef, InsertStatus.DRAFT);
		// the message should now be stored with the new date
		// there are already messages for that day
		Assert.assertEquals(1, msgId3);
		Assert.assertNull(store.getLocalMessage(id1, msgDate3, msgId3));
		Assert.assertEquals("<msg3_v3/>", store.getLocalMessage(id1, msgDate1, msgId3));
	}
}
