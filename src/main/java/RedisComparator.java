import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.*;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.codec.RedisCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class RedisComparator {
	private final static Logger LOG = LoggerFactory.getLogger(ComparisonContext.class);

	private final RedisURI srcUri;
	private final RedisURI dstUri;

	private final RedisCodec<byte[], byte[]> codec = ByteArrayCodec.INSTANCE;

	public RedisComparator(RedisURI srcUri, RedisURI dstUri) {
		this.srcUri = srcUri;
		this.dstUri = dstUri;
	}

	public void compare(DataOutputStream out) {
		RedisClient client = RedisClient.create();

		try (StatefulRedisConnection<byte[], byte[]> srcConn = client.connect(codec, srcUri);
		     StatefulRedisConnection<byte[], byte[]> dstConn = client.connect(codec, dstUri)) {
			new ComparisonContext(srcConn.sync(), dstConn.sync(), out).compare();
		}
	}

	@FunctionalInterface
	private interface ValueComparator {
		Optional<DiffReason> compareValuesOf(byte[] key);

		/***
		 * Chains comparators. The chain stops execution on the fist comparator that
		 * returns a non empty diff.
		 */
		default ValueComparator andThen(ValueComparator after) {
			return (byte[] key) -> {
				Optional<DiffReason> diffReason = compareValuesOf(key);
				if (diffReason.isPresent()) {
					return diffReason;
				} else {
					return after.compareValuesOf(key);
				}
			};
		}
	}

	@FunctionalInterface
	private interface CollectionSizer<K, V> {
		/***
		 * Returns size of collection marked by key in redis db represented by comm.
		 */
		long collectionSizeIn(RedisCommands<K, V> comm, K key);
	}

	private static final Set<String> knownDataTypes = new HashSet<>(Arrays.asList(
		"string",
		"set",
		"zset",
		"hash",
		"list"
	));

	/***
	 * Returns first argument that is not a known data type, nothing otherwise.
	 */
	private static Optional<String> validateDataTypes(String... types) {
		for (String t : types) {
			if (!knownDataTypes.contains(t)) {
				return Optional.of(t);
			}
		}

		return Optional.empty();
	}

	private class ComparisonContext {
		private final RedisCommands<byte[], byte[]> src;
		private final RedisCommands<byte[], byte[]> dst;
		private final DataOutputStream              out;

		private Map<String, ValueComparator> valueComparators = new HashMap<>();

		private ComparisonContext(
			RedisCommands<byte[], byte[]> src,
			RedisCommands<byte[], byte[]> dst,
			DataOutputStream out) {
			this.src = src;
			this.dst = dst;
			this.out = out;

			initComparators();
		}

		private ValueComparator buildSizeComparator(CollectionSizer<byte[], byte[]> sizer) {
			return key -> {
				long srcSize = sizer.collectionSizeIn(src, key);
				long dstSize = sizer.collectionSizeIn(dst, key);

				if (srcSize != dstSize) {
					return Optional.of(new SizeDiff(key, srcSize, dstSize));
				} else {
					return Optional.empty();
				}
			};
		}

		private void initComparators() {
			valueComparators.put(
				"string",
				key -> {
					byte[] srcVal = src.get(key);
					byte[] dstVal = dst.get(key);

					if (!Arrays.equals(srcVal, dstVal)) {
						return Optional.of(new ValueDiff(key, srcVal, dstVal));
					} else {
						return Optional.empty();
					}
				}
			);

			valueComparators.put(
				"hash",
				buildSizeComparator(RedisHashCommands::hlen)
					.andThen(key -> {
						ScanCursor mapCursor = ScanCursor.INITIAL;
						while (!mapCursor.isFinished()) {
							MapScanCursor<byte[], byte[]> mapCurrCursor = src.hscan(key, mapCursor);

							ArrayList<Map.Entry<byte[], byte[]>> mapEntries = new ArrayList<>(mapCurrCursor.getMap().entrySet());

							List<byte[]> dstValues = dst.hmget(key, Utils.getKeys(mapEntries).toArray(new byte[0][]));
							// This should be true even without checking for collection size beforehand
							assert (mapEntries.size() == dstValues.size());

							Iterator<Map.Entry<byte[], byte[]>> entriesIt = mapEntries.iterator();
							Iterator<byte[]> dstValuesIt = dstValues.iterator();

							while (entriesIt.hasNext() && dstValuesIt.hasNext()) {
								Map.Entry<byte[], byte[]> entry = entriesIt.next();
								byte[] srcVal = entry.getValue();
								byte[] dstVal = dstValuesIt.next();

								if (!Arrays.equals(srcVal, dstVal)) {
									return Optional.of(new HashValueDiff(key, entry.getKey(), srcVal, dstVal));
								}
							}

							mapCursor = mapCurrCursor;
						}

						return Optional.empty();
					})
			);

			valueComparators.put(
				"set",
				buildSizeComparator(RedisSetCommands::scard)
					.andThen(key -> {
						ScanCursor setCursor = ScanCursor.INITIAL;
						while (!setCursor.isFinished()) {
							ValueScanCursor<byte[]> currSetCursor = src.sscan(key, setCursor);

							List<byte[]> srcValues = currSetCursor.getValues();

							for (byte[] srcVal : srcValues) {
								if (!dst.sismember(key, srcVal)) {
									return Optional.of(new SetValueMissingInDst(key, srcVal));
								}
							}

							setCursor = currSetCursor;
						}

						return Optional.empty();
					})
			);

			valueComparators.put(
				"zset",
				buildSizeComparator(RedisSortedSetCommands::zcard)
					.andThen(key -> {
						ScanCursor zsetCursor = ScanCursor.INITIAL;
						while (!zsetCursor.isFinished()) {
							ScoredValueScanCursor<byte[]> currZsetCursor = src.zscan(key, zsetCursor);

							List<ScoredValue<byte[]>> srcValues = currZsetCursor.getValues();

							for (ScoredValue<byte[]> srcScoredVal : srcValues) {
								Double dstScore = dst.zscore(key, srcScoredVal.value);

								if (dstScore == null) {
									return Optional.of(new SetValueMissingInDst(key, srcScoredVal.value));
								}

								if (srcScoredVal.score != dstScore) {
									return Optional.of(new SetValueScoreDiff(key, srcScoredVal.score, dstScore));
								}
							}

							zsetCursor = currZsetCursor;
						}

						return Optional.empty();
					})
			);

			valueComparators.put(
				"list",
				buildSizeComparator(RedisListCommands::llen)
					.andThen(key -> {
						final long numItemsPerQuery = 100;
						for (long i = 0; i < Long.MAX_VALUE; i += numItemsPerQuery) {
							List<byte[]> srcValues = src.lrange(key, i, i + numItemsPerQuery);
							List<byte[]> dstValues = dst.lrange(key, i, i + numItemsPerQuery);

							// At this point we have verified that lists are of the same length
							assert (srcValues != null && dstValues != null);
							assert (srcValues.size() == dstValues.size());

							// We may have exhausted list range
							if (srcValues.size() > 0) {
								for (int j = 0; j < srcValues.size(); ++j) {
									byte[] srcVal = srcValues.get(j);
									byte[] dstVal = dstValues.get(j);

									if (!Arrays.equals(srcVal, dstVal)) {
										return Optional.of(new ListValueDiff(key, srcVal, dstVal, j));
									}
								}
							}
						}

						return Optional.empty();
					})
			);
		}

		private void compare() {
			ScanCursor srcCursor = ScanCursor.INITIAL;
			while (!srcCursor.isFinished()) {
				KeyScanCursor<byte[]> currSrcCursor = src.scan(srcCursor);

				LOG.info("scan #keys: {}", currSrcCursor.getKeys().size());

				for (byte[] key : currSrcCursor.getKeys()) {
					String srcKeyType = src.type(key);
					String dstKeyType = dst.type(key);

					if (dstKeyType.equals("none")) {
						onDiff(new KeyDoesntExistInDst(key));
					} else if (!srcKeyType.equals(dstKeyType)) {
						onDiff(new ValueTypeDiff(key, srcKeyType, dstKeyType));
					} else {
						Optional<String> unkownType = validateDataTypes(srcKeyType, dstKeyType);
						if (unkownType.isPresent()) {
							LOG.warn("Skipping key {}, unknown data type: {}", Utils.byteArrayToString(key), unkownType.get());
							continue;
						}

						getValueComparatorFor(srcKeyType)
							.compareValuesOf(key)
							.ifPresent(this::onDiff);
					}
				}

				srcCursor = currSrcCursor;
			}
		}

		private void onDiff(DiffReason reason) {
			try {
				reason.write(out);
				out.write("\n".getBytes());
			} catch (IOException e) {
				LOG.warn("", e);
			}
		}

		private ValueComparator getValueComparatorFor(String valueType) {
			return valueComparators.getOrDefault(
				valueType,
				key -> {
					assert (false);
					LOG.error("Skipping key {}, no suitable comparator found for key type", Utils.byteArrayToString(key));
					return Optional.empty();
				}
			);
		}
	}

//	public class Diff {
//		private List<DiffReason> diff = new ArrayList<>();
//
//		public void add(DiffReason reason) {
//			diff.add(reason);
//		}
//
//		public int size() {
//			return diff.size();
//		}
//
//		public void write(DataOutputStream out) throws IOException {
//			for (DiffReason dr : diff) {
//				dr.write(out);
//				out.writeChar('\n');
//			}
//		}
//	}

	private abstract class DiffReason {
		private final byte[] key;

		protected DiffReason(byte[] key) {
			this.key = key;
		}

		public RedisURI srcUri() {
			return srcUri;
		}

		public RedisURI dstUri() {
			return dstUri;
		}

		protected String srcStr() {
			return srcUri().toURI().toString();
		}

		protected String dstStr() {
			return dstUri().toURI().toString();
		}

		public void write(DataOutputStream out) throws IOException {
			out.write(key);
			out.write(" >> ".getBytes());
		}
	}

	public class KeyDoesntExistInDst extends DiffReason {
		public KeyDoesntExistInDst(byte[] key) {
			super(key);
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(String.format("does not exist on %s", dstStr()).getBytes());
		}
	}

	public class ValueTypeDiff extends DiffReason {
		private final String srcType;
		private final String dstType;

		public ValueTypeDiff(byte[] key, String srcType, String dstType) {
			super(key);
			this.srcType = srcType;
			this.dstType = dstType;
		}

		public String srcType() {
			return srcType;
		}

		public String dstType() {
			return dstType;
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(
				String.format("type '%s' on %s, '%s' on %s",
					srcType(), srcStr(),
					dstType(), dstStr())
					.getBytes()
			);
		}
	}

	public class ValueDiff extends DiffReason {
		private final byte[] srcVal;
		private final byte[] dstVal;

		public ValueDiff(byte[] key, byte[] srcVal, byte[] dstVal) {
			super(key);
			this.srcVal = srcVal;
			this.dstVal = dstVal;
		}

		public byte[] srcVal() {
			return srcVal;
		}

		public byte[] dstVal() {
			return dstVal;
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(
				String.format("value '%s' on %s, '%s' on %s",
					Arrays.toString(srcVal), srcStr(),
					Arrays.toString(dstVal), dstStr())
					.getBytes()
			);
		}
	}

	public class ListValueDiff extends DiffReason {
		private final byte[] srcVal;
		private final byte[] dstVal;
		private final int    index;

		protected ListValueDiff(byte[] key, byte[] srcVal, byte[] dstVal, int index) {
			super(key);
			this.srcVal = srcVal;
			this.dstVal = dstVal;
			this.index = index;
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(
				String.format("[at index %d] value '%s' on %s, '%s' on %s",
					index,
					Arrays.toString(srcVal), srcStr(),
					Arrays.toString(dstVal), dstStr())
					.getBytes()
			);
		}
	}

	public class HashValueDiff extends DiffReason {
		private final byte[] hashKey;
		private final byte[] srcVal;
		private final byte[] dstVal;

		protected HashValueDiff(byte[] key, byte[] hashKey, byte[] srcVal, byte[] dstVal) {
			super(key);
			this.hashKey = hashKey;
			this.srcVal = srcVal;
			this.dstVal = dstVal;
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(
				String.format("[hash key '%s'] value '%s' on %s, '%s' on %s",
					Arrays.toString(hashKey),
					Arrays.toString(srcVal), srcStr(),
					Arrays.toString(dstVal), dstStr())
					.getBytes()
			);
		}
	}

	public class SetValueMissingInDst extends DiffReason {
		private final byte[] missingValue;

		protected SetValueMissingInDst(byte[] key, byte[] missingValue) {
			super(key);
			this.missingValue = missingValue;
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(
				String.format("missing value '%s' from set on %s",
					Arrays.toString(missingValue), dstStr())
					.getBytes()
			);
		}
	}

	public class SetValueScoreDiff extends DiffReason {
		private final double srcScore;
		private final double dstScore;

		protected SetValueScoreDiff(byte[] key, double srcScore, double dstScore) {
			super(key);
			this.srcScore = srcScore;
			this.dstScore = dstScore;
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(
				String.format("set value score difference: %f on %s, %f on %s",
					srcScore, srcStr(),
					dstScore, dstStr())
					.getBytes()
			);
		}
	}

	public class SizeDiff extends DiffReason {
		private final long srcSize;
		private final long dstSize;

		public SizeDiff(byte[] key, long srcSize, long dstSize) {
			super(key);
			this.srcSize = srcSize;
			this.dstSize = dstSize;
		}

		public long srcSize() {
			return srcSize;
		}

		public long dstSize() {
			return dstSize;
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(
				String.format("size %d on %s, %d on %s",
					srcSize, srcStr(),
					dstSize, dstStr())
					.getBytes()
			);
		}
	}

	public class TtlDiff extends DiffReason {
		private final long srcTtl;
		private final long dstTtl;

		protected TtlDiff(byte[] key, long srcTtl, long dstTtl) {
			super(key);
			this.srcTtl = srcTtl;
			this.dstTtl = dstTtl;
		}

		@Override
		public void write(DataOutputStream out) throws IOException {
			super.write(out);
			out.write(
				String.format("ttl %d on %s, %d on %s",
					srcTtl, srcStr(),
					dstTtl, dstStr())
					.getBytes()
			);
		}
	}
}
