package jfms.fcp;

public interface FcpListener {
	void error(String fcpIdentifier, int code);
	void redirect(String fcpIdentifier, String redirectURI);
	void finished(String fcpIdentifier, byte[] data);
	void putSuccessful(String fcpIdentifier, String key);
	void keyPairGenerated(String fcpIdentifier, String publicKey, String privateKey);
}
