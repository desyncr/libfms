package jfms.fms;

import java.time.LocalDate;

import jfms.config.Constants;

public class AddedInfo {
	private final LocalDate dateAdded;
	private final int addedBy;

	public AddedInfo(LocalDate dateAdded, int addedBy) {
		this.dateAdded = dateAdded;
		this.addedBy = addedBy;
	}

	public LocalDate getDateAdded() {
		return dateAdded;
	}

	public int getAddedBy() {
		return addedBy;
	}

	public String getAddedByAsString() {
		if (addedBy >= 0) {
			Identity id = FmsManager.getInstance().getIdentityManager()
				.getIdentity(addedBy);
			if (id != null) {
				return "trust list of " + id.getFullName();
			}
		}

		String addedStr;
		switch (addedBy) {
		case Constants.ADD_SEED_IDENTITY:
			addedStr = "Seed identity";
			break;
		case Constants.ADD_MANUALLY:
			addedStr = "Manually Added";
			break;
		case Constants.ADD_PUZZLE_SOLVED:
			addedStr = "Solved introduction puzzle";
			break;
		case Constants.ADD_IMPORT:
			addedStr = "FMS import";
			break;
		default:
			addedStr = "unknown";
		}

		return addedStr;
	}
}
