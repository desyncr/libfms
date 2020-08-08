package jfms.fms;

public interface ProgressListener {
	void updateProgress(long workDone, long max);
	void updateTitle(String title);
	void updateMessage(String message);
}
