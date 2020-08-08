package jfms.util;

public class Version {
	public static int compare(String v1, String v2) {
		String[] fields1 = v1.split("\\.");
		String[] fields2 = v2.split("\\.");

		for (int i=0; i<fields1.length; i++) {
			if (fields2.length <= i) {
				return 1;
			}

			int value1 = Integer.parseInt(fields1[i]);
			int value2 = Integer.parseInt(fields2[i]);

			if (value1 != value2) {
				return value1 - value2;
			}
		}

		if (fields1.length < fields2.length) {
			return -1;
		}

		return 0;
	}

	private Version() {
	}
}
