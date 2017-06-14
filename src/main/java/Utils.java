import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {
	public static String byteArrayToString(byte[] arr) {
		try {
			return new String(arr, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return Arrays.toString(arr);
		}
	}

	/***
	 * Returns true if only one of the values is null.
	 */
	public static <V, M> boolean nullXor(V v, M m) {
		return (v != null && m == null) ||
		       (v == null && m != null);
	}

	/***
	 * Returns true if one or more of the arguments is (are) null
	 */
	public static <T> boolean notNull(T... args) {
		for (T a : args) {
			if (a == null) {
				return false;
			}
		}

		return true;
	}

	public static <K, V> List<K> getKeys(List<Map.Entry<K, V>> entries) {
		return entries.stream().map(Map.Entry::getKey).collect(Collectors.toList());
	}
}
