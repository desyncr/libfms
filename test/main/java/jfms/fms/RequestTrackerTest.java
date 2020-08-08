package jfms.fms;

import java.io.File;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jfms.config.Config;
import jfms.config.Constants;
import jfms.store.Store;

public class RequestTrackerTest {

	private static final String TEST_DB = "test.db3";
	private static final int ID = 2;
	private static final int MAX_INDEX = 3;
	private LocalDate currentDate;
	private Store store;

	@Before
	public void setUp() throws SQLException {
		new File(TEST_DB).delete();

		currentDate = LocalDate.parse("2018-02-20", DateTimeFormatter.ISO_LOCAL_DATE);
		store = new Store("jdbc:sqlite:" + TEST_DB);
		store.initialize(null);
		FmsManager.getInstance().setStore(store);

		Config.getInstance().setStringValue(Config.MAX_IDENTITY_AGE,
				Constants.DEFAULT_MAX_IDENTITY_AGE);
	}

	@Test
	public void testMinusDays() throws SQLException {
		final int maxIdentityAge =
			Config.getInstance().getMaxIdentityAge();
		final LocalTime time = LocalTime.parse("12:00:00",
				DateTimeFormatter.ISO_LOCAL_TIME);
		LocalDate stopDate = currentDate.minusDays(maxIdentityAge + 1);
		RequestType type = RequestType.IDENTITY;

		RequestTracker spec;
		spec = RequestTracker.create(type, ID, currentDate, MAX_INDEX);
		Assert.assertEquals(currentDate, spec.getDate());
		Assert.assertEquals(0, spec.getIndex());
		Assert.assertEquals(currentDate, spec.getStartDate());
		Assert.assertEquals(stopDate, spec.getStopDate());
		Assert.assertNull(spec.getFailDate());

		// fail on current day is not stored
		spec.setFail(LocalDateTime.of(currentDate, time));
		Assert.assertNull(spec.getFailDate());
		spec = spec.minusDays(1);

		for (int i = 0; i < maxIdentityAge; i++) {
			// fail on previous day
			spec.setFail(LocalDateTime.of(currentDate, time));
			Assert.assertEquals(currentDate.minusDays(i + 1), spec.getDate());
			Assert.assertEquals(0, spec.getIndex());
			Assert.assertEquals(currentDate, spec.getStartDate());
			Assert.assertEquals(currentDate.minusDays(maxIdentityAge + 1),
					spec.getStopDate());
			Assert.assertEquals(currentDate.minusDays(1), spec.getFailDate());

			spec = spec.minusDays(1);
		}

		Assert.assertNull(spec);
	}

	@Test
	public void testIncrementIndex() throws SQLException {
		LocalDate stopDate = currentDate.minusDays(
				Config.getInstance().getMaxIdentityAge() + 1);
		RequestType type = RequestType.IDENTITY;

		RequestTracker spec;
		spec = RequestTracker.create(type, ID, currentDate, MAX_INDEX);
		Assert.assertEquals(currentDate, spec.getDate());
		Assert.assertEquals(0, spec.getIndex());
		Assert.assertEquals(currentDate, spec.getStartDate());
		Assert.assertEquals(stopDate, spec.getStopDate());
		Assert.assertNull(spec.getFailDate());

		for (int i = 0; i < MAX_INDEX; i++) {
			spec = spec.incrementIndex();
			Assert.assertEquals(currentDate, spec.getDate());
			Assert.assertEquals(i + 1, spec.getIndex());
			Assert.assertEquals(currentDate, spec.getStartDate());
			Assert.assertEquals(stopDate, spec.getStopDate());
			Assert.assertNull(spec.getFailDate());
		}

		spec = spec.incrementIndex();
		Assert.assertNull(spec);
	}

	@Test
	public void testStopDate() throws SQLException {
		LocalDate stopDate = currentDate.minusDays(3);
		RequestType type = RequestType.IDENTITY;

		store.updateLastFailDate(ID, stopDate);

		RequestTracker spec;
		spec = RequestTracker.create(type, ID, currentDate, MAX_INDEX);
		Assert.assertEquals(currentDate, spec.getDate());
		Assert.assertEquals(0, spec.getIndex());
		Assert.assertEquals(currentDate, spec.getStartDate());
		Assert.assertEquals(stopDate, spec.getStopDate());
		Assert.assertNull(spec.getFailDate());

		for (int i = 0; i < 2; i++) {
			spec = spec.minusDays(1);
			Assert.assertEquals(currentDate.minusDays(i + 1), spec.getDate());
			Assert.assertEquals(0, spec.getIndex());
			Assert.assertEquals(currentDate, spec.getStartDate());
			Assert.assertEquals(stopDate, spec.getStopDate());
			Assert.assertNull(spec.getFailDate());
		}

		spec = spec.minusDays(1);
		Assert.assertNull(spec);
	}

	@Test
	public void testSuccesfulToday() throws SQLException {
		LocalDate stopDate = currentDate.minusDays(1);
		RequestType type = RequestType.IDENTITY;

		store.updateRequestHistory(ID, type, currentDate, 0);

		RequestTracker spec;
		spec = RequestTracker.create(type, ID, currentDate, MAX_INDEX);
		Assert.assertEquals(currentDate, spec.getDate());
		Assert.assertEquals(1, spec.getIndex());
		Assert.assertEquals(currentDate, spec.getStartDate());
		Assert.assertEquals(stopDate, spec.getStopDate());
		Assert.assertNull(spec.getFailDate());
	}

	@Test
	public void testSuccessfulPreviously() throws SQLException {
		final LocalTime time = LocalTime.parse("02:59:59",
				DateTimeFormatter.ISO_LOCAL_TIME);

		LocalDate stopDate = currentDate.minusDays(3);
		RequestType type = RequestType.IDENTITY;

		store.updateRequestHistory(ID, type, currentDate.minusDays(2), 0);

		RequestTracker spec;
		spec = RequestTracker.create(type, ID, currentDate, MAX_INDEX);
		Assert.assertEquals(currentDate, spec.getDate());
		Assert.assertEquals(0, spec.getIndex());
		Assert.assertEquals(currentDate, spec.getStartDate());
		Assert.assertEquals(stopDate, spec.getStopDate());
		Assert.assertNull(spec.getFailDate());

		spec.setFail(LocalDateTime.of(currentDate, time));
		spec = spec.minusDays(1);
		Assert.assertEquals(currentDate.minusDays(1), spec.getDate());
		Assert.assertEquals(0, spec.getIndex());
		Assert.assertEquals(currentDate, spec.getStartDate());
		Assert.assertEquals(stopDate, spec.getStopDate());
		Assert.assertNull(spec.getFailDate());

		spec.setFail(LocalDateTime.of(currentDate, time));
		spec = spec.minusDays(1);
		Assert.assertEquals(currentDate.minusDays(2), spec.getDate());
		Assert.assertEquals(1, spec.getIndex());
		Assert.assertEquals(currentDate, spec.getStartDate());
		Assert.assertEquals(stopDate, spec.getStopDate());
		Assert.assertNull(spec.getFailDate());

		spec = spec.minusDays(1);
		Assert.assertNull(spec);
	}

	@Test
	public void testFailAfterLastSuccess() throws SQLException {
		LocalDate stopDate = currentDate.minusDays(1);
		RequestType type = RequestType.IDENTITY;

		store.updateRequestHistory(ID, type, currentDate.minusDays(7), 0);
		store.updateLastFailDate(ID, stopDate);

		RequestTracker spec;
		spec = RequestTracker.create(type, ID, currentDate, MAX_INDEX);
		Assert.assertEquals(currentDate, spec.getDate());
		Assert.assertEquals(0, spec.getIndex());
		Assert.assertEquals(currentDate, spec.getStartDate());
		Assert.assertEquals(stopDate, spec.getStopDate());
		Assert.assertNull(spec.getFailDate());
	}
}
