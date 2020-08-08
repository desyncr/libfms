package jfms.fms;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import jfms.config.Constants;
import jfms.store.Store;

public class RequestTracker {
	private final LocalDate date;
	private final int index;
	private final int maxIndex;
	private final LocalDate startDate;
	private final LocalDate stopDate;
	private final DateIndex lastRequest;
	private LocalDate failDate;
	private int successCount;
	private boolean fastMessageCheckEnabled;

	public static RequestTracker create(LocalDate date, int maxIndex) {
		LocalDate stopDate = date.minusDays(1);
		DateIndex lastRequest = new DateIndex(Constants.FALLBACK_DATE, -1);
		return new RequestTracker(date, 0, maxIndex,
				date, stopDate, null, lastRequest, 0, false);
	}

	public static RequestTracker create(RequestType type, int identityId,
			LocalDate date, int maxIndex) {

		Store store = FmsManager.getInstance().getStore();
		LocalDate stopDate = date.minusDays(
				Integer.parseInt(Constants.DEFAULT_MAX_IDENTITY_AGE) + 1);

		DateIndex lastRequest = store.getLastRequestDateIndex(identityId, type);
		if (lastRequest.getIndex() >= 0) {
			LocalDate lastSuccessDate = lastRequest.getDate().minusDays(1);
			if (stopDate.isBefore(lastSuccessDate)) {
				stopDate = lastSuccessDate;
			}
		}

		LocalDate lastFailDate = store.getLastFailDate(identityId);
		if (stopDate.isBefore(lastFailDate)) {
			stopDate = lastFailDate;
		}


		int compVal = date.compareTo(lastRequest.getDate());
		if (compVal < 0) {
			// date < last date
			return null;
		} else if (compVal == 0) {
			// date == last date
			if (lastRequest.getIndex() < maxIndex) {
				return new RequestTracker(date,
						lastRequest.getIndex() + 1, maxIndex,
						date, stopDate, null, lastRequest, 0, false);
			} else {
				return null;
			}
		} else {
			// date > last date
			return new RequestTracker(date, 0, maxIndex,
					date, stopDate, null, lastRequest, 0, false);
		}
	}

	public static RequestTracker createSingleRequest(RequestType type,
			int identityId, LocalDate date) {

		Store store = FmsManager.getInstance().getStore();

		DateIndex lastRequest = store.getLastRequestDateIndex(identityId, type);
		if (date.compareTo(lastRequest.getDate()) <= 0) {
			// date is before or equal to last date
			return null;
		}

		LocalDate stopDate = date.minusDays(1);
		return new RequestTracker(date, 0, 0,
				date, stopDate, null, lastRequest, 0, false);
	}


	private RequestTracker(LocalDate date, int index, int maxIndex,
			LocalDate startDate, LocalDate stopDate, LocalDate failDate,
			DateIndex lastRequest, int successCount, boolean fastMessageCheckEnabled) {

		this.date = date;
		this.index = index;
		this.maxIndex = maxIndex;
		this.startDate = startDate;
		this.stopDate = stopDate;
		this.failDate = failDate;
		this.lastRequest = lastRequest;
		this.successCount = successCount;
		this.fastMessageCheckEnabled = fastMessageCheckEnabled;
	}

	public LocalDate getDate() {
		return date;
	}

	public LocalDate getStartDate() {
		return startDate;
	}

	public LocalDate getStopDate() {
		return stopDate;
	}

	public LocalDate getFailDate() {
		return failDate;
	}

	public RequestTracker setFailDate(LocalDate failDate) {
		return new RequestTracker(date, index, maxIndex, startDate, stopDate,
				failDate, lastRequest, successCount, fastMessageCheckEnabled);
	}

	public int getIndex() {
		return index;
	}

	public RequestTracker incrementIndex() {
		if (index < maxIndex) {
			return setIndex(index + 1);
		} else {
			return null;
		}
	}

	public RequestTracker setIndex(int index) {
		return new RequestTracker(date, index, maxIndex, startDate, stopDate,
				failDate, lastRequest, successCount, fastMessageCheckEnabled);
	}

	public RequestTracker minusDays(long daysToSubstract) {
		// if index is greater zero we have received at least one request
		// for the day
		if (index > 0) {
			return null;
		}

		final LocalDate newDate = date.minusDays(daysToSubstract);
		if (!newDate.isAfter(stopDate)) {
			return null;
		}

		int newIndex = 0;
		if (newDate.equals(lastRequest.getDate())) {
			newIndex = lastRequest.getIndex() + 1;
			if (newIndex >= maxIndex) {
				return null;
			}
		}

		// retry previous day with index 0
		return new RequestTracker(newDate, newIndex, maxIndex,
				startDate, stopDate, failDate, lastRequest, successCount,
				fastMessageCheckEnabled);
	}

	public void setFail() {
		setFail(LocalDateTime.now(ZoneOffset.UTC));
	}

	public void setFail(LocalDateTime dateTime) {
		if (failDate != null || index > 0) {
			return;
		}

		LocalDate today = dateTime.toLocalDate();
		if (date.equals(today)) {
			return;
		}

		if (date.equals(today.minusDays(1))) {
			// give client/network some time until yesterday's data is
			// available before setting day to failed
			if (dateTime.getHour() >= 3) {
				failDate = date;
			}
		} else {
			failDate = date;
		}
	}

	public void setSuccessCount(int successCount) {
		this.successCount = successCount;
	}

	public void setSuccess() {
		if (successCount >= 0) {
			successCount++;
		}
	}

	public int getSuccessCount() {
		return successCount;
	}

	public void setFastMessageCheckEnabled(boolean fastMessageCheckEnabled) {
		this.fastMessageCheckEnabled = fastMessageCheckEnabled;
	}

	public boolean getFastMessageCheckEnabled() {
		return fastMessageCheckEnabled;
	}
}
