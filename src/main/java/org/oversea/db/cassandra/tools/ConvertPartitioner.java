package org.oversea.db.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.oversea.db.cassandra.json.sorter.SSTableJsonExternalSorter;
import org.oversea.utils.TimeUtils;

public class ConvertPartitioner {
	private static final String INFILE_OPTION = "i";
	private static final String OUTFILE_OPTION = "o";
	private static final String PARTITIONER_OPTION = "p";
	private static final String ROW_PER_CHUNK_OPTION = "r";
	private static final String TEMP_DIR_OPTION = "d";
	private static final String DEBUG_OPTION = "x";
	private static final Options options = new Options();
	private static CommandLine cmd;
	static {
		Option optInput = new Option(INFILE_OPTION, true, "Input file path");
		optInput.setRequired(true);
		options.addOption(optInput);

		Option optOutput = new Option(OUTFILE_OPTION, true, "Output file path");
		optOutput.setRequired(true);
		options.addOption(optOutput);

		Option optChunkSize = new Option(ROW_PER_CHUNK_OPTION, true,
				"Number of row per one chunk file. Default: 1000");
		optChunkSize.setRequired(false);
		options.addOption(optChunkSize);

		Option optTempDir = new Option(TEMP_DIR_OPTION, true,
				"Temp Folder. Default:/tmp");
		optTempDir.setRequired(false);
		options.addOption(optTempDir);

		Option optDebug = new Option(DEBUG_OPTION, false, "Debug");
		optDebug.setRequired(false);
		options.addOption(optDebug);

		Option optFromCF = new Option(PARTITIONER_OPTION, true,
				"Partitioner Class. Example:"
						+ "\n\t org.apache.cassandra.dht.Murmur3Partitioner"
						+ "\n\t org.apache.cassandra.dht.RandomPartitioner");
		optFromCF.setRequired(true);

		options.addOption(optFromCF);

	}

	public static void main(String[] args) throws ClassNotFoundException,
			SecurityException, NoSuchMethodException, IllegalArgumentException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException, IOException {
		CommandLineParser parser = new PosixParser();

		try {
			cmd = parser.parse(options, args);
			File inFile = new File(cmd.getOptionValue(INFILE_OPTION));
			File outFile = new File(cmd.getOptionValue(OUTFILE_OPTION));

			int rowPerChunk = Integer.parseInt(cmd.getOptionValue(
					ROW_PER_CHUNK_OPTION, "1000"));
			File tmpDir = new File(cmd.getOptionValue(TEMP_DIR_OPTION, "/tmp"));
			String partionerClassName = cmd.getOptionValue(PARTITIONER_OPTION);
			boolean debug = cmd.hasOption(DEBUG_OPTION);
			Class<?> c = Class.forName(partionerClassName);
			Constructor<?> cons = c.getConstructor();
			IPartitioner<?> partitioner = (IPartitioner<?>) cons.newInstance();
			// IPartitioner<?> partitioner = new Murmur3Partitioner();
			SSTableJsonExternalSorter sorter = new SSTableJsonExternalSorter(
					partitioner, tmpDir, rowPerChunk, 10, debug);
			System.out.println("SORTING...");
			long begin = System.currentTimeMillis();
			sorter.sort(inFile, outFile);
			System.out.println("Finish");
			long end = System.currentTimeMillis();
			System.out.println("Handle time:"
					+ TimeUtils.showNiceTime(end - begin));
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println(e.getMessage());
			printProgramUsage();
			System.exit(1);
		}
	}

	private static void printProgramUsage() {
		System.out
				.println("Usage: -i <inFile> -o <outFile> -p <partitioner> [-d <temp folder>] [-r <row per tempfile>] [-x]");

		System.out.println("Options:");
		for (Object o : options.getOptions()) {
			Option opt = (Option) o;
			System.out.println("  -" + opt.getOpt() + " - "
					+ opt.getDescription());
		}

	}
}
