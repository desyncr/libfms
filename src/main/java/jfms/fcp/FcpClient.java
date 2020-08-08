package jfms.fcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import jfms.config.Constants;

public class FcpClient {
	private static final Logger LOG = Logger.getLogger(FcpClient.class.getName());

	private final String name;
	private final String host;
	private final int port;
	private final ByteBuffer buffer = ByteBuffer.allocate(4096);
	private boolean dataNeeded = true;
	private final AtomicBoolean stop = new AtomicBoolean();
	private volatile SocketChannel channel;
	private volatile Thread receiverThread;

	// TODO merge FcpListener and FcpStatusListener?
	private final Map<String, FcpListener> listenerMap = new ConcurrentHashMap<>();
	private FcpStatusListener statusListener;

	private final RequestCache requestCache = new RequestCache();
	private final Map<String, TTLData> ttlMap = new ConcurrentHashMap<>();

	public enum Status {
		CONNECTED,
		DISCONNECTED,
		CONNECT_FAILED
	}

	private static class TTLData {
		private final String uri;
		private final int ttl;

		public TTLData(String uri, int ttl) {
			this.uri = uri;
			this.ttl = ttl;
		}

		public String getURI() {
			return uri;
		}

		public int getTTL() {
			return ttl;
		}
	}

	public FcpClient(String name, String host, int port) {
		this.name = name + '-' + UUID.randomUUID().toString();
		this.host = host;
		this.port = port;
	}

	private class Receiver implements Runnable {
		@Override
		public void run() {
			LOG.log(Level.FINE, "Starting FCP receiver thread with ID {0}",
					Thread.currentThread().getId());

			stop.set(false);
			try {
				while (!stop.get()) {
					try {
						receiveAndHandleResponse();
					} catch (ClosedByInterruptException e) {
						LOG.log(Level.FINE, "FCP receiver thread interrupted");
						stop.set(true);
					}
				}
			} catch (IOException|FcpException e) {
				LOG.log(Level.WARNING, "Exception in FCP thread", e);

				statusListener.statusChanged(Status.DISCONNECTED);
			} catch (Exception e) {
				LOG.log(Level.WARNING, "Unexpected exception in FCP thread", e);
				statusListener.statusChanged(Status.DISCONNECTED);
			}

			closeChannel();

			LOG.log(Level.FINE, "FCP receiver stopped");
		}
	}

	public synchronized void start() throws FcpException {
		if (receiverThread != null) {
			LOG.log(Level.FINEST, "FCP receiver thread already running");
			return;
		}

		if (!connectChannel()) {
			LOG.log(Level.WARNING,
					"Failed to connect to {0}:{1,number,0}", new Object[]{
					host, port});
			throw new FcpException("FCP connect failed");
		}

		receiverThread = new Thread(new Receiver());
		receiverThread.start();
	}

	public synchronized void cleanup() {
		requestCache.cleanup();
	}

	public synchronized void shutdown() {
		if (receiverThread == null) {
			LOG.log(Level.FINEST, "FCP receiver thread not running");
			return;
		}

		LOG.log(Level.FINE, "Interrupting FCP receiver thread...");
		stop.set(true);
		receiverThread.interrupt();
		receiverThread = null;
	}

	public void setStatusListener(FcpStatusListener listener) {
		statusListener = listener;
	}

	public boolean isRecentlyFailed(String key) {
		return requestCache.isPresent(key);
	}

	public synchronized void requestKey(String identifier, String key,
			FcpListener listener, int ttl) throws FcpException {

		requestKey(identifier, key, listener, ttl, null, 0);
	}

	public synchronized boolean requestKey(String identifier, String key,
										   FcpListener listener, int ttl, List<String> additionalFields) throws FcpException {
		return requestKey(identifier, key, listener, ttl, null, 0);
	}

	public synchronized boolean requestKey(String identifier, String key,
			FcpListener listener, int ttl, List<String> additionalFields, int priority)
		throws FcpException {

		if (requestCache.isPresent(key)) {
			LOG.log(Level.FINEST, "Skipping recently failed key {0}", key);
			return false;
		}

		try {
			listenerMap.put(identifier, listener);
			if (ttl > 0) {
				ttlMap.put(identifier, new TTLData(key, ttl));
			}
			sendClientGet(identifier, key, additionalFields, priority);
			return true;
		} catch (IOException e) {
			throw new FcpException("ClientGet failed", e);
		}
	}
	public synchronized void insertKey(String identifier, String key,
									   byte[] data, FcpListener listener) throws FcpException {
		insertKey(identifier, key, data, listener, 0);
    }

	public synchronized void insertKey(String identifier, String key,
			byte[] data, FcpListener listener, int priority) throws FcpException {
		try {
			listenerMap.put(identifier, listener);
			sendClientPut(identifier, key, data, priority);
		} catch (IOException e) {
			throw new FcpException("ClientPut failed", e);
		}
	}
	public synchronized void insertDirectory(String identifier, String key,
											 FcpDirectoryEntry[] files, String defaultName,
											 FcpListener listener, List<String> additionalFields) throws FcpException {
		insertDirectory(identifier, key, files, defaultName, listener, additionalFields, 0);
	}

	public synchronized void insertDirectory(String identifier, String key,
			FcpDirectoryEntry[] files, String defaultName,
			FcpListener listener, List<String> additionalFields, int priority)
		throws FcpException {

		try {
			listenerMap.put(identifier, listener);
			sendClientPutComplexDir(identifier, key, files, defaultName,
					additionalFields, priority);
		} catch (IOException e) {
			throw new FcpException("ClientPutComplexDir failed", e);
		}
	}

	public synchronized void generateKeyPair(String identifier,
			FcpListener listener) throws FcpException {

		try {
			listenerMap.put(identifier, listener);
			sendGenerateSSK(identifier);
		} catch (IOException e) {
			throw new FcpException("generateSsk failed", e);
		}
	}

	public synchronized void cancel(String identifier) throws FcpException {
		try {
			sendRemoveRequest(identifier);
		} catch (IOException e) {
			throw new FcpException("RemoveRequest failed", e);
		}
	}

	protected Map<String, FcpListener> getListenerMap() {
		return listenerMap;
	}

	protected FcpListener getListener(String identifier) {
		return listenerMap.get(identifier);
	}

	protected void sendClientHello() throws FcpException, IOException {
		LOG.log(Level.FINEST, "[FCP] ClientHello Name={0}", name);

		StringBuilder str = new StringBuilder("ClientHello\n");
		str.append("Name=");
		str.append(name);
		str.append('\n');
		str.append("ExpectedVersion=2.0\n");
		str.append("EndMessage\n");

		byte[] header = str.toString().getBytes(StandardCharsets.US_ASCII);
		sendData(header);
	}

	protected void sendClientGet(String identifier, String key,
			List<String> additionalFields, int priority) throws IOException {

		LOG.log(Level.FINEST,
				"[FCP] ClientGet Identifier={0} URI={1}", new Object[]{
				identifier, key});

		StringBuilder str = new StringBuilder("ClientGet\n");
		str.append("URI=");
		str.append(key);
		str.append('\n');
		str.append("Identifier=");
		str.append(identifier);
		str.append('\n');
		str.append("Verbosity=0\n");
		str.append("MaxSize=");
		str.append(Constants.MAX_REQUEST_SIZE);
		str.append('\n');
		str.append("ReturnType=direct\n");

		final int prio = priority;// Config.getInstance().getDownloadPriority();
		if (prio >= 0) {
			str.append("PriorityClass=");
			str.append(prio);
			str.append('\n');
		}

		if (additionalFields != null) {
			for (String field : additionalFields) {
				str.append(field);
				str.append('\n');
			}
		}
		str.append("EndMessage\n");

		byte[] header = str.toString().getBytes(StandardCharsets.US_ASCII);
		sendData(header);
	}

	protected void sendClientPut(String identifier, String key, byte[] data, int priority)
	throws IOException {
		LOG.log(Level.FINEST,
				"[FCP] ClientPut Identifier={0} URI={1}", new Object[]{
				identifier, key});

		StringBuilder str = new StringBuilder("ClientPut\n");
		str.append("URI=");
		str.append(key);
		str.append('\n');
		str.append("Identifier=");
		str.append(identifier);
		str.append('\n');
		str.append("Verbosity=0\n");
		str.append("UploadFrom=direct\n");

		final int prio = priority; // libfms changed: Config.getInstance().getUploadPriority();
		if (prio >= 0) {
			str.append("PriorityClass=");
			str.append(prio);
			str.append('\n');
		}

		str.append("DataLength=");
		str.append(data.length);
		str.append('\n');
		str.append("EndMessage\n");

		byte[] header = str.toString().getBytes(StandardCharsets.US_ASCII);
		sendData(header);
		sendData(data);
	}

	protected void sendClientPutComplexDir(String identifier, String key,
			FcpDirectoryEntry[] files, String defaultName,
			List<String> additionalFields, int priority)
		throws IOException {

		LOG.log(Level.FINEST,
				"[FCP] ClientPutComplexDir Identifier={0} URI={1}", new Object[]{
				identifier, key});

		StringBuilder str = new StringBuilder("ClientPutComplexDir\n");
		str.append("URI=");
		str.append(key);
		str.append('\n');

		str.append("Identifier=");
		str.append(identifier);
		str.append('\n');

		str.append("Verbosity=0\n");

		if (defaultName != null) {
			str.append("DefaultName=");
			str.append(defaultName);
			str.append('\n');
		}

		str.append("UploadFrom=direct\n");

		final int prio = priority; // libfms changed: Config.getInstance().getUploadPriority();
		if (prio >= 0) {
			str.append("PriorityClass=");
			str.append(prio);
			str.append('\n');
		}

		if (additionalFields != null) {
			for (String field : additionalFields) {
				str.append(field);
				str.append('\n');
			}
		}

		ByteArrayOutputStream data = new ByteArrayOutputStream();

		for (int i=0; i<files.length; i++) {
			final FcpDirectoryEntry e = files[i];
			str.append("Files.");
			str.append(i);
			str.append(".Name=");
			str.append(e.getName());
			str.append('\n');

			str.append("Files.");
			str.append(i);
			str.append(".UploadFrom=direct\n");

			str.append("Files.");
			str.append(i);
			str.append(".DataLength=");
			str.append(e.getData().length);
			str.append('\n');

			data.write(e.getData());
		}

		str.append("EndMessage\n");

		byte[] header = str.toString().getBytes(StandardCharsets.US_ASCII);
		sendData(header);
		sendData(data.toByteArray());
	}

	protected void sendGenerateSSK(String identifier) throws IOException {

		LOG.log(Level.FINEST, "[FCP] GenerateSSK Identifier={0}", identifier);

		StringBuilder str = new StringBuilder("GenerateSSK\n");
		str.append("Identifier=");
		str.append(identifier);
		str.append('\n');
		str.append("EndMessage\n");

		byte[] header = str.toString().getBytes(StandardCharsets.US_ASCII);
		sendData(header);
	}

	protected void sendRemoveRequest(String identifier) throws IOException {

		LOG.log(Level.FINEST, "[FCP] RemoveRequest Identifier={0}", identifier);

		StringBuilder str = new StringBuilder("RemoveRequest\n");
		str.append("Identifier=");
		str.append(identifier);
		str.append('\n');
		str.append("EndMessage\n");

		byte[] header = str.toString().getBytes(StandardCharsets.US_ASCII);
		sendData(header);
	}

	private boolean connectChannel() throws FcpException {
		LOG.log(Level.FINEST,
				"Trying to connect to {0}:{1,number,0}", new Object[]{
				host, port});

		boolean connected = false;
		try {
			channel = SocketChannel.open(new InetSocketAddress(host, port));
			sendClientHello();

			// TODO add timeout
			FcpResponse helloResponse = receiveResponse();
			if (!helloResponse.getName().equals("NodeHello")) {
				LOG.log(Level.WARNING, "invalid response to ClientHello: {0}",
						helloResponse.getName());
				throw new FcpException("invalid server response");
			}
			LOG.log(Level.INFO, "Node available. Version={0}",
					helloResponse.getField("Revision"));
			statusListener.statusChanged(Status.CONNECTED);
			connected = true;
		} catch (ClosedByInterruptException e) {
			LOG.log(Level.FINE, "connect interrupted");
			closeChannel();
		} catch (FcpException|IOException e) {
			LOG.log(Level.FINE, "connect failed: " + e.getMessage(), e);
			closeChannel();
			statusListener.statusChanged(Status.CONNECT_FAILED);
		}

		return connected;
	}

	private void closeChannel() {
		if (channel != null) {
			try {
				channel.close();
			} catch (IOException e) {
				LOG.log(Level.WARNING, "Failed to close FCP socket", e);
			}
			channel = null;
		}
	}

	private void sendData(byte[] data)
		throws IOException {

		if (channel == null) {
			LOG.log(Level.WARNING, "Trying to send on closed channel");
			return;
		}

		ByteBuffer outBuffer = ByteBuffer.wrap(data);
		while (outBuffer.hasRemaining()) {
			channel.write(outBuffer);
		}
	}

	private void receiveAndHandleResponse() throws IOException, FcpException {
		FcpResponse response = receiveResponse();
		String id = response.getField("Identifier");
		if (id == null) {
			throw new FcpException("Identifier field missing");
		}

		FcpListener listener = listenerMap.get(id);
		if (listener == null) {
			LOG.log(Level.WARNING, "No listener found for request {0}", id);
			return;
		}

		TTLData ttl = ttlMap.remove(id);

		switch (response.getName()) {
		case "AllData":
			LOG.log(Level.FINEST, "[FCP] AllData response Identifier={0}", id);
			listener.finished(id, response.getData());
			listenerMap.remove(id);
			break;
		case "GetFailed":
			String redirectURI = response.getField("RedirectURI");
			if (redirectURI == null) {
				int code = Integer.parseInt(response.getField("Code"));
				listener.error(id, code);
				listenerMap.remove(id);
			} else {
				listener.redirect(id, redirectURI);
			}

			if (ttl != null) {
				requestCache.addNegativeCacheEntry(ttl.getURI(), ttl.getTTL());
			}
			break;
		case "PutFailed":
			int code = Integer.parseInt(response.getField("Code"));
			listener.error(id, code);
			listenerMap.remove(id);
			break;
		case "PutSuccessful":
			listener.putSuccessful(id, response.getField("URI"));
			listenerMap.remove(id);
			break;
		case "SSKKeypair":
			listener.keyPairGenerated(id, response.getField("RequestURI"),
					response.getField("InsertURI"));
			listenerMap.remove(id);
			break;
		default:
			// don't remove from Map, further response for ID expected
			LOG.log(Level.FINEST, "Unhandled {0}", response.getName());
			break;
		}
	}

	private byte[] receiveData(int dataLength)
		throws FcpException, IOException {

		final byte[] data = new byte[dataLength];

		int bytesRead = 0;
		while (bytesRead < dataLength) {
			if (dataNeeded) {
				buffer.clear();
				if (channel.read(buffer) <= 0) {
					throw new FcpException("connection closed");
				}
				buffer.flip();
			}

			int bytesRemaining = dataLength - bytesRead;
			int bytesToCopy = Math.min(bytesRemaining, buffer.remaining());
			buffer.get(data, bytesRead, bytesToCopy);
			bytesRead += bytesToCopy;

			dataNeeded = !buffer.hasRemaining();
		}

		return data;
	}

	private FcpResponse receiveResponse() throws IOException, FcpException {
		LOG.log(Level.FINEST, "Entering receiveResponse");
		String type;
		do {
			type = receiveLine();
		} while(type.isEmpty());
		LOG.log(Level.FINEST, "received {0} response", type);
		FcpResponse response = new FcpResponse(type);

		while (true) {
			String line = receiveLine();
			if (line.equals("EndMessage")) {
				return response;
			}

			if (line.equals("Data")) {
				break;
			}

			String[] fields = line.split("=", 2);
			if (fields.length != 2) {
				continue;
			}

			response.addField(fields[0], fields[1]);
			LOG.log(Level.FINEST, "\t{0}: {1}", new Object[]{fields[0], fields[1]});
		}

		String dataLength = response.getField("DataLength");
		if (dataLength == null) {
			throw new FcpException("DataLength missing in response");
		}

		int len = Integer.parseInt(dataLength);

		byte[] data = receiveData(len);
		response.setData(data);

		return response;
	}

	private String receiveLine()
		throws FcpException, IOException {

		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		boolean newlineFound = false;
		while (!newlineFound) {
			if (dataNeeded) {
				buffer.clear();
				if (channel.read(buffer) <= 0) {
					throw new IOException("connection closed");
				}
				buffer.flip();
			}

			while (buffer.hasRemaining()) {
				final byte ch = buffer.get();
				if (ch == '\n') {
					newlineFound = true;
					break;
				}

				bos.write(ch);
			}

			dataNeeded = !buffer.hasRemaining();
		}

		return new String(bos.toByteArray(), StandardCharsets.US_ASCII);
	}
}
