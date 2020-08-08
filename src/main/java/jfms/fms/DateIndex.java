package jfms.fms;

import java.time.LocalDate;
import java.util.Objects;

public class DateIndex {
	private final LocalDate date;
	private final int index;

	public DateIndex(LocalDate date, int index) {
		this.date = date;
		this.index = index;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof DateIndex) {
			final DateIndex d = (DateIndex)o;
			return Objects.equals(date, d.date) && index == d.index;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return
			13 * Objects.hashCode(this.date) +
			17 * index;
	}

	public LocalDate getDate() {
		return date;
	}

	public int getIndex() {
		return index;
	}
}
