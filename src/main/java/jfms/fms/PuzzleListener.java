package jfms.fms;

public interface PuzzleListener {
	void onPuzzleAdded(IntroductionPuzzle puzzle, String publisher);
	void onError(String message);
}
