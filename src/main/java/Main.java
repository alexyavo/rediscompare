import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.lambdaworks.redis.RedisURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.Optional;

public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	private static final int DefaultRedisPort = 6379;

	private static class Arguments {

		@Parameter(names = "-src-host", required = true)
		public String srcHost;

		@Parameter(names = "-src-port")
		public Integer srcPort = DefaultRedisPort;

		@Parameter(names = "-src-passwd")
		public String srcPasswd = null;

		@Parameter(names = "-src-db")
		public Integer srcDb = null;

		@Parameter(names = "-dst-host", required = true)
		public String dstHost;

		@Parameter(names = "-dst-port")
		public Integer dstPort = DefaultRedisPort;

		@Parameter(names = "-dst-passwd")
		public String dstPasswd = null;

		@Parameter(names = "-dst-db")
		public Integer dstDb = null;

		@Parameter(names = "-out", required = true)
		public String outPath;

		@Parameter(names = "-dbg")
		public Boolean dbg = false;
	}

	public static void main(String[] mainArgs) {
		Arguments args = new Arguments();
		JCommander.newBuilder().addObject(args).build().parse(mainArgs);

		RedisURI.Builder srcBuilder = RedisURI.builder();

		srcBuilder
			.withHost(args.srcHost)
			.withPort(args.srcPort);

		if (args.srcDb != null) {
			srcBuilder.withDatabase(args.srcDb);
		}

		if (args.srcPasswd != null) {
			srcBuilder.withPassword(args.srcPasswd);
		}

		RedisURI.Builder dstBuilder = RedisURI.builder();

		dstBuilder
			.withHost(args.dstHost)
			.withPort(args.dstPort);

		if (args.dstDb != null) {
			dstBuilder.withDatabase(args.dstDb);
		}

		if (args.dstPasswd != null) {
			dstBuilder.withPassword(args.dstPasswd);
		}

		try (DataOutputStream out =
			     new DataOutputStream(
				     Files.newOutputStream(
					     Paths.get(args.outPath),
					     StandardOpenOption.CREATE,
					     StandardOpenOption.TRUNCATE_EXISTING))) {
			new RedisComparator(srcBuilder.build(), dstBuilder.build()).compare(out);
		} catch (IOException e) {
			LOG.error("", e);
		}
	}
}
