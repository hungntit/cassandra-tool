package org.oversea.db.cassandra.json.sorter;

import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.codehaus.jackson.type.TypeReference;
import org.oversea.db.cassandra.json.sorter.ExternalJsonMergeSort.MergeIterator;

public class SSTableJsonExternalSorter {
	private IPartitioner<?> partitioner;
	private File tmpDir;
	private int chunkSize;
	private int maxOpenFiles;
	private boolean debug = false;

	public SSTableJsonExternalSorter(IPartitioner<?> partitioner, File tmpDir,
			int chunkSize, int maxOpenFiles) {
		this(partitioner, tmpDir, chunkSize, maxOpenFiles, false);
	}

	public SSTableJsonExternalSorter(IPartitioner<?> partitioner, File tmpDir,
			int chunkSize, int maxOpenFiles, boolean debug) {
		super();
		this.partitioner = partitioner;
		this.tmpDir = tmpDir;
		this.chunkSize = chunkSize;
		this.maxOpenFiles = maxOpenFiles;
		this.debug = debug;
	}

	/**
	 * sort JSON file by Cassandra partitioner
	 * 
	 * @param inputFile
	 *            : input cassandra json file
	 * @param outputFile
	 *            : output cassandra json file
	 * @throws IOException
	 */
	public void sort(File inputFile, File outputFile) throws IOException {

		// Create comparator
		Comparator<Map<?, ?>> comparator = new Comparator<Map<?, ?>>() {
			public int compare(Map<?, ?> o1, Map<?, ?> o2) {
				DecoratedKey decoratedKey1 = partitioner
						.decorateKey(hexToBytes((String) o1.get("key")));
				DecoratedKey decoratedKey2 = partitioner
						.decorateKey(hexToBytes((String) o2.get("key")));
				return decoratedKey1.compareTo(decoratedKey2);
			}
		};

		// Create serializer
		SSTableJsonRowSerializer<Map<?, ?>> serializer = new SSTableJsonRowSerializer<Map<?, ?>>(
				new TypeReference<Map<?, ?>>() {
				});
		ExternalJsonMergeSort.debug = debug;
		ExternalJsonMergeSort.debugMerge = debug;
		// Create the external merge sort instance
		ExternalJsonMergeSort<Map<?, ?>> sort = ExternalJsonMergeSort
				.newSorter(serializer, comparator).withChunkSize(chunkSize)
				.withMaxOpenFiles(maxOpenFiles).withDistinct(true)
				.withCleanup(true).withTempDirectory(tmpDir).build();

		// Read input file as an input stream and write sorted chunks.
		List<File> sortedChunks;
		InputStream input = new FileInputStream(inputFile);

		try {
			sortedChunks = sort.writeSortedChunks(serializer.readValues(input));
		} finally {
			input.close();
		}

		// Get a merge iterator over the sorted chunks. This will return the
		// objects in sorted order. Note that the sorted chunks will be deleted
		// when the CloseableIterator is closed if 'cleanup' is set to true.
		MergeIterator<Map<?, ?>> sorted = sort.mergeSortedChunks(sortedChunks);
		try {
			if (!outputFile.getParentFile().exists()) {
				outputFile.getParentFile().mkdirs();
				System.out.println("Create folder "
						+ outputFile.getParentFile().getPath());
			}
			OutputStream output = new FileOutputStream(outputFile);
			try {
				System.out.println("Writing result to file "
						+ outputFile.getPath());
				serializer.writeValues(sorted, output);
			} finally {
				output.close();
			}
		} finally {
			sorted.close();
		}

	}

}
