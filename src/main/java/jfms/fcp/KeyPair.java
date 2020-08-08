package jfms.fcp;

public class KeyPair {
	private final String publicKey;
	private final String privateKey;

	public KeyPair(String publicKey, String privateKey) {
		this.publicKey = publicKey;
		this.privateKey = privateKey;
	}

	public String getPublicKey() {
		return publicKey;
	}

	public String getPrivateKey() {
		return privateKey;
	}
}
