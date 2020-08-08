package jfms.fms;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import jfms.config.Constants;
import jfms.store.Store;

public class TrustManager {
	private static final Logger LOG = Logger.getLogger(TrustManager.class.getName());

	private Map<Integer, Integer> localTrustListTrust;
	private Map<Integer, Integer> localMessageTrust;
	private Map<Integer, Integer> peerTrustListTrust;
	private Map<Integer, Integer> peerMessageTrust;

	public static class TrustLevel {
		private int trustSum = 0;
		private int weightSum = 0;

		public void addTrust(int trust, int weight) {
			if (trust >= 0) {
				trustSum += trust * weight;
				weightSum += weight;
			}
		}

		public int getTrust() {
			if (weightSum == 0) {
				return -1;
			}
			return (trustSum + weightSum/2) / weightSum;
		}
	}

	public static int trustLevelToInt(Integer level) {
		if (level != null) {
			return level;
		} else {
			return -1;
		}
	}

	public static Integer intToTrustLevel(int value) {
		if (value >= 0) {
			return value;
		} else {
			return null;
		}
	}

	public TrustManager() {
		peerTrustListTrust = Collections.emptyMap();
		peerMessageTrust = Collections.emptyMap();
	}

	public void initialize() {
		if (localTrustListTrust == null || localMessageTrust == null) {
			Store store = FmsManager.getInstance().getStore();
			int identityId = Integer.parseInt(Constants.DEFAULT_DEFAULT_ID);
			localTrustListTrust = store.getLocalTrustListTrusts(identityId, 0);
			localMessageTrust = store.getLocalMessageTrusts(identityId);
		}

		calculateTrustListTrusts();
		calculateMessageTrusts();
	}

	/**
	 * Get local trust list trust level.
	 * The trust level of the default identity is returned.
	 * @param identityId jfms.store ID of identity
	 * @return trust level if present, -1 otherwise
	 */
	public int getLocalTrustListTrust(int identityId) {
		return trustLevelToInt(localTrustListTrust.get(identityId));
	}

	/**
	 * Get local message trust level.
	 * The trust level of the default identity is returned.
	 * @param identityId jfms.store ID of identity
	 * @return trust level if present, -1 otherwise
	 */
	public int getLocalMessageTrust(int identityId) {
		return trustLevelToInt(localMessageTrust.get(identityId));
	}

	public void updateLocalTrust(int localIdentityId, int identityId,
			Trust trust) {

		FmsManager.getInstance().getStore()
			.updateLocalTrust(localIdentityId, identityId, trust);

		if (localIdentityId == Integer.parseInt(Constants.DEFAULT_DEFAULT_ID)) {
			localTrustListTrust.put(identityId,
					intToTrustLevel(trust.getTrustListTrustLevel()));
			localMessageTrust.put(identityId,
					intToTrustLevel(trust.getMessageTrustLevel()));
		}
	}

	/**
	 * Get peer trust list trust level.
	 * @param identityId jfms.store ID of identity
	 * @return trust level if present, -1 otherwise
	 */
	public int getPeerTrustListTrust(int identityId) {
		return trustLevelToInt(peerTrustListTrust.get(identityId));
	}

	/**
	 * Get peer message trust level.
	 * @param identityId jfms.store ID of identity
	 * @return trust level if present, -1 otherwise
	 */
	public int getPeerMessageTrust(int identityId) {
		return trustLevelToInt(peerMessageTrust.get(identityId));
	}

	public Set<Integer> getTrustListTrustedIds() {
		return getTrustListTrustedIds(-1);
	}

	public Set<Integer> getTrustListTrustedIds(int minPeerTrust) {
		return peerTrustListTrust.keySet().stream()
			.filter(i -> isTrustListTrusted(i, minPeerTrust))
			.collect(Collectors.toSet());
	}

	public Set<Integer> getMessageTrustedIds() {
		Set<Integer> allIds = FmsManager.getInstance().getIdentityManager()
				.getIdentities().keySet();
		boolean includeNullPeerTrust = true; // libfms changed

		return allIds.stream()
			.filter(i -> isMessageTrusted(i, includeNullPeerTrust))
			.collect(Collectors.toSet());
	}

	public boolean isTrustListTrusted(int identityId, int minPeerTrust) {
		Integer localTrust = localTrustListTrust.get(identityId);
		if (localTrust != null) {
			return localTrust >= Integer.parseInt(Constants.DEFAULT_MIN_LOCAL_TRUSTLIST_TRUST);
		}

		if (minPeerTrust < 0) {
			return true;
		}

		Integer peerTrust = peerTrustListTrust.get(identityId);
		if (peerTrust != null) {
			return peerTrust >= minPeerTrust;
		}

		return false;
	}

	public boolean isMessageTrusted(int identityId,
			boolean includeNullPeerTrust) {

		Integer localTrust = localMessageTrust.get(identityId);
		if (localTrust != null) {
			return localTrust >= Integer.parseInt(Constants.DEFAULT_MIN_LOCAL_TRUSTLIST_TRUST);
		}

		Integer peerTrust = peerMessageTrust.get(identityId);
		if (peerTrust != null) {
			return peerTrust >= Integer.parseInt(Constants.DEFAULT_MIN_PEER_MESSAGE_TRUST);
		}

		return includeNullPeerTrust;
	}

	private Map<Integer, Double> calculateTrustIteration(
		Set<Integer> ids,
		Map<Integer,Double> initialTrusts,
		Map<Integer,Double> currentTrusts,
		Map<Integer, Map<Integer, Integer>> trustByTargetId)
	{
		final Map<Integer,Double> nextTrusts = new HashMap<>();
		final double alpha = (double)(Integer.parseInt(Constants.DEFAULT_INDIRECT_TRUST_WEIGHT))/100.0;
		final boolean excludeNullTrust = true;

		// compute reputation as the weighted average of reputation values of
		// aggregated values
		for (Integer targetId : ids) {
			Map<Integer, Integer> targetTrusts = trustByTargetId.get(targetId);
			if (targetTrusts == null) {
				// ID appears in local trust list but in none of the
				// peer trust lists
				targetTrusts = Collections.emptyMap();
			}

			double weightedTrustSum = 0.0;
			double availableTrust = 0.0;
			for (Integer trusterId : ids) {
				Double trusterReputation = currentTrusts.get(trusterId);
				if (trusterReputation == null) {
					continue;
				}
				Integer targetReputation = targetTrusts.get(trusterId);
				double trustLevel;
				if (targetReputation != null) {
					trustLevel = targetReputation.doubleValue() / 100.0;
				} else {
					if (excludeNullTrust) {
						continue;
					}
					trustLevel = 0.5;
				}
				weightedTrustSum += trusterReputation * trustLevel;
				availableTrust += trusterReputation;
			}

			Double initialTrust = initialTrusts.get(targetId);
			if (initialTrust == null) {
				if (availableTrust < 0.01) {
					// neither local trust nor relevant peer trust
					continue;
				}

				if (alpha < 0.01) {
					// avoid giving rating of 50 to unrated identities if
					// alpha is 0
					continue;
				}

				// assume local TLT of 50 if empty
				initialTrust = 0.5;
			}


			double weightedAverage = 0.0;
			if (availableTrust >= 0.01) {
				weightedAverage = weightedTrustSum/availableTrust;
			}

			// calculate the average over direct and indirect evidence
			Double nextTrust =
				(1.0 - alpha) * initialTrust + alpha * weightedAverage;

			nextTrusts.put(targetId, nextTrust);
		}

		return nextTrusts;
	}


	/**
	 * Calculate trust list trusts.
	 * The algorithm is based on
	 * Simone, A., Škorić, B., Zannone, N.: Flow-based reputation: more than
	 * just ranking. International Journal of Information Technology and
	 * Decision Making, Vol. 11, No. 3, p.551-578.
	 *
	 * If EXCLUDE_NULL_TRUST is set to false, the algorithm as described in the
	 * paper is used. As there are only very few ratings available, most trust
	 * levels end up very close to 50.
	 *
	 * Therefor, EXCLUDE_NULL_TRUST is set to true by default. NULL trust
	 * values will be excluded from trust level calculation. This gives a much
	 * wider range of trust levels. Unfortunately, we will lose some
	 * properties of the original algorithm, e.g., a single identity will
	 * dominate the trust levels of identities with few ratings.
	 */
	private void calculateTrustListTrusts() {
		final Store store = FmsManager.getInstance().getStore();

		int localIdentityId = Integer.parseInt(Constants.DEFAULT_DEFAULT_ID);
		LOG.log(Level.FINE,
				"Calculating reputations for local ID {0} with alpha {1}",
				new Object[]{localIdentityId, Constants.DEFAULT_INDIRECT_TRUST_WEIGHT});

		final Map<Integer, Map<Integer, Integer>> trustByTargetId =
			store.getPeerTrusts();

		Set<Integer> ids = new HashSet<>(trustByTargetId.keySet());
		LOG.log(Level.FINEST, "Found peer ratings for {0} identities",
				ids.size());

		Map<Integer, Integer> localTrust = localTrustListTrust;
		if (!localTrust.isEmpty()) {
			LOG.log(Level.FINEST, "Found {0} identities in local trustlist",
					localTrust.size());
		} else {
			LOG.log(Level.INFO, "No IDs with local trustlist trust found, "
					+ "using seed identities");

			localTrust = new HashMap<>();
			IdentityManager identityManager =
				FmsManager.getInstance().getIdentityManager();
			for (String ssk : store.getSeedIdentitySsks()) {
				Integer id = identityManager.getIdentityId(ssk);
				if (id != null) {
					localTrust.put(id, Constants.DEFAULT_SEED_TRUST);
				}
			}

		}
		ids.addAll(localTrust.keySet());

		LOG.log(Level.FINEST, "Using {0} identities for trustlist calculation",
				ids.size());


		for (Integer targetId : ids) {
			Map<Integer, Integer> targetTrusts = trustByTargetId
				.computeIfAbsent(targetId, e -> new HashMap<>());
			// override self-trust
			targetTrusts.put(targetId, 0);
		}

		// fill with existing values from local trustlist trust
		final Map<Integer,Double> initialTrusts = new HashMap<>();

		for (Map.Entry<Integer, Integer> e : localTrust.entrySet()) {
			Integer id = e.getKey();
			Integer trust = e.getValue();

			initialTrusts.put(id, trust.doubleValue()/100.0);

			// In the original algorithm, self-trust is always set to 0,
			// If EXCLUDE_NULL_TRUST is enabled, we relax this for locally
			// trusted IDs, otherwise unstable behavior was observed, e.g. seed
			// identities might up with NULL trust level and levels might
			// diverge
			if (true) {
				Map<Integer, Integer> targetTrusts = trustByTargetId.get(id);
				targetTrusts.put(id, trust);
			}
		}


		if (!true) {
			// set all trust levels to neutral (0.5) if there is no rating
			final Double neutralTrust = 0.5;
			for (Integer id : ids) {
				initialTrusts.computeIfAbsent(id, e -> neutralTrust);
			}
		}

		Map<Integer,Double> currentTrusts = new HashMap<>(initialTrusts);

		double diff = 1.0;
		final double threshold = 10e-15;
		int iteration = 1;
		while (diff > threshold) {
			Map<Integer,Double> nextTrusts = calculateTrustIteration(
					ids, initialTrusts, currentTrusts, trustByTargetId);

			double oldSum = currentTrusts.values().stream()
				.filter(Objects::nonNull)
				.mapToDouble(Double::doubleValue).sum();

			double newSum = nextTrusts.values().stream()
				.filter(Objects::nonNull)
				.mapToDouble(Double::doubleValue).sum();

			diff = Math.abs(newSum - oldSum);

			currentTrusts = nextTrusts;

			LOG.log(Level.FINEST,"Trustlist calculation iteration {0}: "
					+ "delta={1,number,#.###############}",
					new Object[]{iteration, diff});

			// According to the paper, the original algorithm typically
			// terminates in 12 or less iterations. Unfortunately, it may get
			// stuck in a loop if EXCLUDE_NULL_TRUST is set and the modified
			// algorithm is used.
			if (iteration >= 64) {
				LOG.log(Level.INFO, "aborting trustlist calculation");
				break;
			}
			iteration++;
		}

		peerTrustListTrust = currentTrusts.entrySet().stream()
			.filter(e -> e.getValue() != null)
			.collect(Collectors.toMap(Map.Entry::getKey,
						e -> (int)Math.round(e.getValue() * 100.0)));
	}

	private void calculateMessageTrusts() {
		Store store = FmsManager.getInstance().getStore();

		final int minPeerTrustlistTrust = Integer.parseInt(Constants.DEFAULT_MIN_PEER_TRUSTLIST_TRUST);
		Map<Integer, TrustLevel> calculatedTrusts = new HashMap<>();

		for (Map.Entry<Integer, Integer> e : peerTrustListTrust.entrySet()) {
			int identityId = e.getKey();
			if (!isTrustListTrusted(identityId, minPeerTrustlistTrust)) {
				continue;
			}

			int trustListTrust = e.getValue();
			List<Trust> trustList = store.getTrustList(identityId);

			for (Trust t : trustList) {
				if (t.getMessageTrustLevel() >= 0) {
					int trustedIdentityId = t.getIdentityId();

					TrustLevel tl = calculatedTrusts.computeIfAbsent(
							trustedIdentityId, k -> new TrustLevel());
					tl.addTrust(t.getMessageTrustLevel(), trustListTrust);
				}
			}
		}

		peerMessageTrust = calculatedTrusts.entrySet()
			.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getTrust()));
	}
}
