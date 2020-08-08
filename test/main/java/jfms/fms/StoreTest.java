package jfms.fms;

import java.io.File;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jfms.config.Constants;
import static jfms.fms.RequestType.IDENTITY;
import jfms.store.InsertStatus;
import jfms.store.Store;

public class StoreTest {
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
	public void testGetRecentSsks() throws SQLException {
		LocalDate date = LocalDate.parse("2018-01-11",
				DateTimeFormatter.ISO_LOCAL_DATE);

		int id1 = addManualIdentity("SSK1");
		store.updateRequestHistory(id1, RequestType.IDENTITY,
				date.minusDays(1), 0);

		int id2 = addManualIdentity("SSK2");
		store.updateRequestHistory(id2, RequestType.IDENTITY,
				date.minusDays(2), 0);

		Assert.assertArrayEquals(new String[0],
				store.getRecentSsks(date).toArray());

		Assert.assertArrayEquals(new String[] {"SSK1"},
				store.getRecentSsks(date.minusDays(1)).toArray());

		Assert.assertArrayEquals(new String[] {"SSK1", "SSK2"},
				store.getRecentSsks(date.minusDays(2)).toArray());

		Assert.assertArrayEquals(new String[] {"SSK1", "SSK2"},
				store.getRecentSsks(date.minusDays(3)).toArray());
	}

	@Test
	public void testIdentityInsert() throws SQLException {
		testUpdateInsert(RequestType.IDENTITY);
		testIncrementIndex(RequestType.IDENTITY);
	}

	@Test
	public void testTrustListInsert() throws SQLException {
		testUpdateInsert(RequestType.TRUST_LIST);
		testIncrementIndex(RequestType.TRUST_LIST);
	}

	@Test
	public void testMessageListInsert() throws SQLException {
		testUpdateInsert(RequestType.MESSAGE_LIST);
		testIncrementIndex(RequestType.MESSAGE_LIST);
	}

	@Test
	public void testRequestTypes() throws SQLException {
		int id = 1;
		LocalDate date = LocalDate.parse("2018-01-01", DateTimeFormatter.ISO_LOCAL_DATE);
		InsertStatus status = InsertStatus.IGNORE;

		store.updateInsert(RequestType.IDENTITY, id, date, 2, status);
		store.updateInsert(RequestType.TRUST_LIST, id, date, 3, status);
		store.updateInsert(RequestType.MESSAGE_LIST, id, date, 5, status);

		Assert.assertEquals(2,
				store.getInsertIndex(RequestType.IDENTITY,	id, date, status));
		Assert.assertEquals(3,
				store.getInsertIndex(RequestType.TRUST_LIST, id, date, status));
		Assert.assertEquals(5,
				store.getInsertIndex(RequestType.MESSAGE_LIST,	id, date, status));
	}

	@Test
	public void testInsertStatus() throws SQLException {
		RequestType type = RequestType.TRUST_LIST;
		int id = 1;
		LocalDate date = LocalDate.parse("2018-01-01", DateTimeFormatter.ISO_LOCAL_DATE);

		store.updateInsert(type, id, date, 0, InsertStatus.NOT_INSERTED);
		Assert.assertEquals(0, store.getInsertIndex(type, id, date,
				InsertStatus.IGNORE));
		Assert.assertEquals(-1, store.getInsertIndex(type, id, date,
				InsertStatus.INSERTED));
		Assert.assertEquals(0, store.getInsertIndex(type, id, date,
				InsertStatus.NOT_INSERTED));

		store.updateInsert(type, id, date, 0, InsertStatus.INSERTED);
		Assert.assertEquals(0, store.getInsertIndex(type, id, date,
				InsertStatus.IGNORE));
		Assert.assertEquals(0, store.getInsertIndex(type, id, date,
				InsertStatus.INSERTED));
		Assert.assertEquals(-1, store.getInsertIndex(type, id, date,
				InsertStatus.NOT_INSERTED));
	}

	@Test
	public void testIntroductionPuzzle() throws SQLException {
		int id = 1;
		LocalDate date = LocalDate.parse("2018-01-01", DateTimeFormatter.ISO_LOCAL_DATE);

		int unsolvedCount;
		int nextIndex;

		unsolvedCount = store.getUnsolvedPuzzleCount(id, date);
		Assert.assertEquals(0, unsolvedCount);

		nextIndex = store.getNextIntroductionPuzzleIndex(id, date);
		Assert.assertEquals(0, nextIndex);

		IntroductionPuzzle puzzle = new IntroductionPuzzle();
		puzzle.setUUID("UUID1");
		puzzle.setSolution("solution1");

		// insert puzzle at index 0
		store.saveIntroductionPuzzle(id, date, nextIndex, puzzle);

		unsolvedCount = store.getUnsolvedPuzzleCount(id, date);
		Assert.assertEquals(1, unsolvedCount);

		nextIndex = store.getNextIntroductionPuzzleIndex(id, date);
		Assert.assertEquals(1, nextIndex);

		// insert puzzle at index 1
		puzzle.setUUID("UUID2");
		puzzle.setSolution("solution2");
		store.saveIntroductionPuzzle(id, date, nextIndex, puzzle);

		unsolvedCount = store.getUnsolvedPuzzleCount(id, date);
		Assert.assertEquals(2, unsolvedCount);

		nextIndex = store.getNextIntroductionPuzzleIndex(id, date);
		Assert.assertEquals(2, nextIndex);

		// solve puzzle 0
		store.setPuzzleSolved(id, date, 0);

		unsolvedCount = store.getUnsolvedPuzzleCount(id, date);
		Assert.assertEquals(1, unsolvedCount);

		nextIndex = store.getNextIntroductionPuzzleIndex(id, date);
		Assert.assertEquals(2, nextIndex);

		// solve puzzle 1
		store.setPuzzleSolved(id, date, 1);

		unsolvedCount = store.getUnsolvedPuzzleCount(id, date);
		Assert.assertEquals(0, unsolvedCount);

		nextIndex = store.getNextIntroductionPuzzleIndex(id, date);
		Assert.assertEquals(2, nextIndex);
	}

	@Test
	public void testGetUnsolvedPuzzles() throws SQLException {
		int id = 1;
		LocalDate date = LocalDate.parse("2018-01-01", DateTimeFormatter.ISO_LOCAL_DATE);
		IntroductionPuzzle puzzle;


		// insert puzzle at index 1
		IntroductionPuzzle puzzle1 = new IntroductionPuzzle();
		puzzle1.setUUID("UUID1");
		puzzle1.setSolution("solution1");
		store.saveIntroductionPuzzle(id, date, 1, puzzle1);

		Map<DateIndex, IntroductionPuzzle> puzzles;
		puzzles = store.getUnsolvedPuzzles(id, date);

		Assert.assertEquals(1, puzzles.size());
		puzzle = puzzles.get(new DateIndex(date, 1));
		Assert.assertNotNull(puzzle);
		Assert.assertEquals("UUID1", puzzle.getUuid());
		Assert.assertEquals("solution1", puzzle.getSolution());

		// insert puzzle at index 2
		IntroductionPuzzle puzzle2 = new IntroductionPuzzle();
		puzzle2.setUUID("UUID2");
		puzzle2.setSolution("solution2");
		store.saveIntroductionPuzzle(id, date, 2, puzzle2);

		puzzles = store.getUnsolvedPuzzles(id, date);
		Assert.assertEquals(2, puzzles.size());

		puzzle = puzzles.get(new DateIndex(date, 1));
		Assert.assertNotNull(puzzle);
		Assert.assertEquals("UUID1", puzzle.getUuid());
		Assert.assertEquals("solution1", puzzle.getSolution());

		puzzle = puzzles.get(new DateIndex(date, 2));
		Assert.assertNotNull(puzzle);
		Assert.assertEquals("UUID2", puzzle.getUuid());
		Assert.assertEquals("solution2", puzzle.getSolution());

		// solve puzzle 1
		store.setPuzzleSolved(id, date, 1);

		puzzles = store.getUnsolvedPuzzles(id, date);
		Assert.assertEquals(1, puzzles.size());

		puzzle = puzzles.get(new DateIndex(date, 1));
		Assert.assertNull(puzzle);

		puzzle = puzzles.get(new DateIndex(date, 2));
		Assert.assertNotNull(puzzle);
		Assert.assertEquals("UUID2", puzzle.getUuid());
		Assert.assertEquals("solution2", puzzle.getSolution());

		// solve puzzle 2
		store.setPuzzleSolved(id, date, 2);

		puzzles = store.getUnsolvedPuzzles(id, date);
		Assert.assertEquals(0, puzzles.size());
	}

	@Test
	public void testIdentityIntroduction() throws SQLException {
		int id = 1;
		LocalDate date = LocalDate.parse("2018-01-01", DateTimeFormatter.ISO_LOCAL_DATE);

		List<IdentityIntroduction> idIntros;

		idIntros = store.getIdentityIntroductions(id, InsertStatus.NOT_INSERTED);
		Assert.assertEquals(0, idIntros.size());

		// insert first IdentityIntroduction
		store.saveIdentityIntroduction(id, date, "UUID1", "solution1");
		idIntros = store.getIdentityIntroductions(id, InsertStatus.NOT_INSERTED);
		Assert.assertEquals(1, idIntros.size());
		IdentityIntroduction idIntro = idIntros.get(0);
		Assert.assertNotNull(idIntro);
		Assert.assertEquals(date, idIntro.getDate());
		Assert.assertEquals("UUID1", idIntro.getUuid());
		Assert.assertEquals("solution1", idIntro.getSolution());

		// insert second IdentityIntroduction
		store.saveIdentityIntroduction(id, date, "UUID2", "solution2");
		idIntros = store.getIdentityIntroductions(id, InsertStatus.NOT_INSERTED);
		Assert.assertEquals(2, idIntros.size());

		// set first IdentityIntroduction inserted
		store.setIdentityIntroductionInserted(id, date, "UUID1");
		idIntros = store.getIdentityIntroductions(id, InsertStatus.NOT_INSERTED);
		Assert.assertEquals(1, idIntros.size());
		idIntro = idIntros.get(0);
		Assert.assertNotNull(idIntro);
		Assert.assertEquals(date, idIntro.getDate());
		Assert.assertEquals("UUID2", idIntro.getUuid());
		Assert.assertEquals("solution2", idIntro.getSolution());
	}

	@Test
	public void testGetActiveIdentities() {
		// current date is 2018-02-20
		// inactive if not seen for 7 days (since 2018-02-13)
		final LocalDate activeSince = date("2018-02-13");
		// retry if not failed for 3 days (since 2018-02-17)
		final LocalDate notFailedSince = date("2018-02-17");
		// singleUseAddedSince
		final LocalDate su = date("2018-02-13");

		List<Integer> activeIds;
		List<Integer> inactiveIds;

		// new identity without request history
		int id1 = addManualIdentity("SSK1");

		// -> active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(1, activeIds.size());
		Assert.assertTrue(activeIds.contains(id1));

		// -> not inactive
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince,su );
		Assert.assertTrue(inactiveIds.isEmpty());


		// recently seen; fail not set
		int id2 = addManualIdentity("SSK2");
		store.updateRequestHistory(id2, IDENTITY, date("2018-02-13"), 0);

		// -> active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(2, activeIds.size());
		Assert.assertTrue(activeIds.contains(id2));

		// -> not inactive
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince, su);
		Assert.assertTrue(inactiveIds.isEmpty());


		// recently seen; recently failed
		int id3 = addManualIdentity("SSK3");
		store.updateRequestHistory(id3, IDENTITY, date("2018-02-13"), 0);
		store.updateLastFailDate(id3, date("2018-02-17"));

		// -> active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(3, activeIds.size());
		Assert.assertTrue(activeIds.contains(id3));

		// -> not inactive
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince, su);
		Assert.assertTrue(inactiveIds.isEmpty());


		// recently seen; not recently failed
		int id4 = addManualIdentity("SSK4");
		store.updateRequestHistory(id4, IDENTITY, date("2018-02-13"), 0);
		store.updateLastFailDate(id4, date("2018-02-16"));

		// -> active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(4, activeIds.size());
		Assert.assertTrue(activeIds.contains(id4));

		// -> not inactive
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince, su);
		Assert.assertTrue(inactiveIds.isEmpty());


		// not recently seen; fail not set
		int id5 = addManualIdentity("SSK5");
		store.updateRequestHistory(id5, IDENTITY, date("2018-02-12"), 0);

		// -> not active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(4, activeIds.size());
		Assert.assertFalse(activeIds.contains(id5));

		// -> inactive
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince, su);
		Assert.assertEquals(1, inactiveIds.size());
		Assert.assertTrue(inactiveIds.contains(id5));


		// not recently seen; recently failed
		int id6 = addManualIdentity("SSK6");
		store.updateRequestHistory(id6, IDENTITY, date("2018-02-12"), 0);
		store.updateLastFailDate(id6, date("2018-02-17"));

		// -> not active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(4, activeIds.size());
		Assert.assertFalse(activeIds.contains(id6));

		// -> neither inactive because not recently failed
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince, su);
		Assert.assertEquals(1, inactiveIds.size());
		Assert.assertFalse(inactiveIds.contains(id6));


		// not recently seen; not recently failed
		int id7 = addManualIdentity("SSK7");
		store.updateRequestHistory(id7, IDENTITY, date("2018-02-12"), 0);
		store.updateLastFailDate(id7, date("2018-02-16"));

		// -> not active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(4, activeIds.size());
		Assert.assertFalse(activeIds.contains(id7));

		// -> inactive
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince, su);
		Assert.assertEquals(2, inactiveIds.size());
		Assert.assertTrue(inactiveIds.contains(id7));


		// recently seen not set; recently failed
		int id8 = addManualIdentity("SSK8");
		store.updateLastFailDate(id8, date("2018-02-17"));

		// -> not active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(4, activeIds.size());
		Assert.assertFalse(activeIds.contains(id8));

		// -> neither inactive because not recently failed
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince, su);
		Assert.assertEquals(2, inactiveIds.size());
		Assert.assertFalse(inactiveIds.contains(id8));


		// recently seen not set; not recently failed
		int id9 = addManualIdentity("SSK9");
		store.updateLastFailDate(id9, date("2018-02-16"));

		// -> not active
		activeIds = store.getActiveIdentities(activeSince, su);
		Assert.assertEquals(4, activeIds.size());
		Assert.assertFalse(activeIds.contains(id9));

		// -> inactive
		inactiveIds = store.getInactiveIdentities(activeSince, notFailedSince, su);
		Assert.assertEquals(3, inactiveIds.size());
		Assert.assertTrue(inactiveIds.contains(id9));
	}

	@Test
	public void testSingleUseIdentities() {
		// current date is 2018-02-20
		final LocalDate activeSince = date("2018-02-20");
		final LocalDate singleUseAddedSince = date("2018-02-13");

		List<Integer> activeIds;

		// new identity without request history
		// identity added 7 days ago (limit for single use identities)
		int id1 = addManualIdentity("SSK1", date("2018-02-13"));
		store.updateRequestHistory(id1, IDENTITY, date("2018-02-20"), 0);

		// -> active
		activeIds = store.getActiveIdentities(activeSince, singleUseAddedSince);
		Assert.assertEquals(1, activeIds.size());
		Assert.assertTrue(activeIds.contains(id1));

		// mark identity as single use
		Identity identity = new Identity();
		identity.setSingleUse(true);
		store.updateIdentity(id1, identity);

		// -> active
		activeIds = store.getActiveIdentities(activeSince, singleUseAddedSince);
		Assert.assertEquals(1, activeIds.size());
		Assert.assertTrue(activeIds.contains(id1));

		// new identity without request history
		// identity added 8 days ago (before limit for single use identities)
		int id2 = addManualIdentity("SSK2", date("2018-02-12"));
		store.updateRequestHistory(id1, IDENTITY, date("2018-02-20"), 0);

		// -> active
		activeIds = store.getActiveIdentities(activeSince, singleUseAddedSince);
		Assert.assertEquals(2, activeIds.size());
		Assert.assertTrue(activeIds.contains(id2));

		// mark identity as single use
		Identity identity2 = new Identity();
		identity2.setSingleUse(true);
		store.updateIdentity(id2, identity2);

		// -> ignored
		activeIds = store.getActiveIdentities(activeSince, singleUseAddedSince);
		Assert.assertEquals(1, activeIds.size());
		Assert.assertFalse(activeIds.contains(id2));
	}

	private int addManualIdentity(String ssk) {
		return store.saveIdentity(Constants.ADD_MANUALLY, ssk);
	}

	private int addManualIdentity(String ssk, LocalDate date) {
		return store.saveIdentity(Constants.ADD_MANUALLY, ssk, date);
	}


	private void testUpdateInsert(RequestType type) throws SQLException {
		int id = 1;
		LocalDate date = LocalDate.parse("2018-01-01", DateTimeFormatter.ISO_LOCAL_DATE);
		InsertStatus status = InsertStatus.IGNORE;

		// set initial index to 0
		store.updateInsert(type, id, date, 0, status);
		Assert.assertEquals(0, store.getInsertIndex(type, id, date, status));

		// update index
		store.updateInsert(type, id, date, 42, status);
		Assert.assertEquals(42, store.getInsertIndex(type,	id, date, status));
	}

	private void testIncrementIndex(RequestType type) throws SQLException {
		int id = 2;
		LocalDate date = LocalDate.parse("2018-01-01", DateTimeFormatter.ISO_LOCAL_DATE);
		InsertStatus status = InsertStatus.IGNORE;

		// try to increment non-existing index
		store.incrementInsertIndex(type, id, date, 0);
		Assert.assertEquals(-1, store.getInsertIndex(type,	id, date, status));

		// set initial index to 0
		store.updateInsert(type, id, date, 0, status);
		Assert.assertEquals(0, store.getInsertIndex(type, id, date, status));

		// increment index
		store.incrementInsertIndex(type, id, date, 0);
		Assert.assertEquals(1, store.getInsertIndex(type,	id, date, status));
	}
}
