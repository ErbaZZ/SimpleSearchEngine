
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict = new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict = new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue = new LinkedList<File>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	/*
	 * Write a posting list to the given file You should record the file position of this posting list so that you can read it back during retrieval
	 * 
	 */
	private static void writePosting(FileChannel fc, PostingList posting) throws IOException {
		postingDict.put(posting.getTermId(), new Pair<Long, Integer>(fc.size(), posting.getList().size()));
		index.writePosting(fc, posting);
	}

	private static PostingList mergePosting(PostingList p1, PostingList p2) {
		// Default Case - return null if two posting are not for the same term
		if (p1.getTermId() != p2.getTermId()) {
			return null;
		}

		PostingList pNew = new PostingList(p1.getTermId());
		int pointer1 = 0;
		int pointer2 = 0;
		List<Integer> docIdList1 = p1.getList();
		List<Integer> docIdList2 = p2.getList();

		// Merging
		while (!(pointer1 >= p1.getList().size()) && !(pointer2 >= p2.getList().size())) {
			if (docIdList1.get(pointer1) == docIdList2.get(pointer2)) {
				pNew.getList().add(docIdList1.get(pointer1));
				pointer1++;
				pointer2++;
			}
			else if (docIdList1.get(pointer1) < docIdList2.get(pointer2)) {
				pNew.getList().add(docIdList1.get(pointer1));
				pointer1++;
			}
			else {
				pNew.getList().add(docIdList2.get(pointer2));
				pointer2++;
			}
		}

		// Adding Leftover docIds
		while (pointer1 < p1.getList().size()) {
			pNew.getList().add(docIdList1.get(pointer1));
			pointer1++;
		}

		while (pointer2 < p2.getList().size()) {
			pNew.getList().add(docIdList2.get(pointer2));
			pointer2++;
		}

		return pNew;
	}

	/**
	 * Pop next element if there is one, otherwise return null
	 * 
	 * @param iter
	 *            an iterator that contains integers
	 * @return next element or null
	 */
	private static Integer popNextOrNull(Iterator<Integer> iter) {
		if (iter.hasNext()) {
			return iter.next();
		}
		else {
			return null;
		}
	}

	/**
	 * Main method to start the indexing process.
	 * 
	 * @param method
	 *            :Indexing method. "Basic" by default, but extra credit will be given for those who can implement variable byte (VB) or Gamma index
	 *            compression algorithm
	 * @param dataDirname
	 *            :relative path to the dataset root directory. E.g. "./datasets/small"
	 * @param outputDirname
	 *            :relative path to the output directory to store index. You must not assume that this directory exist. If it does, you must clear out
	 *            the content before indexing.
	 */
	public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException {
		/* Get index */
		String className = method + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		}
		catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		File rootdir = new File(dataDirname);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + dataDirname);
			return -1;
		}

		/* Get output directory */
		File outdir = new File(outputDirname);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + outputDirname);
			return -1;
		}

		/* delete all the files/sub folder under outdir */
		if (outdir.exists()) {
			ArrayList<File> fileList = new ArrayList<File>();
			File[] fileArray = outdir.listFiles();
			for (File f : fileArray) {
				if (f.isFile()) {
					f.delete();
				}
				else {
					fileList.add(f);
				}
			}
			while (!fileList.isEmpty()) {
				File tempFile = fileList.get(0);
				if (tempFile.listFiles().length != 0) {
					fileArray = tempFile.listFiles();
					for (File f : fileArray) {
						if (f.isFile()) {
							f.delete();
						}
						else {
							fileList.add(f);
						}
					}
					fileList.add(tempFile);
				}
				else {
					tempFile.delete();
				}
				fileList.remove(0);
			}
		}

		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return -1;
			}
		}

		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();

		/* For each block */
		for (File block : dirlist) {
			// Term id -> PostingList
			Map<Integer, PostingList> postingMap = new TreeMap<Integer, PostingList>();
			File blockFile = new File(outputDirname, block.getName());
			System.out.println("Processing block " + block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();

			/* For each file */
			for (File file : filelist) {
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();

				// use pre-increment to ensure docID > 0
				int docId = ++docIdCounter;
				docDict.put(fileName, docId);

				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						int termId;

						// Get termId from the termDict if exists
						if (termDict.containsKey(token)) {
							termId = termDict.get(token);
						}
						// Create new termId if not exists
						else {
							termId = ++wordIdCounter;
							termDict.put(token, termId);
						}
						// Create new posting ArrayList if not exists
						if (!postingMap.containsKey(termId)) {
							postingMap.put(termId, new PostingList(termId));
							postingMap.get(termId).getList().add(docId);
						}
						// Add docId only if it isn't already exists
						else if (!postingMap.get(termId).getList().contains(docId)) {
							postingMap.get(termId).getList().add(docId);
						}
					}
				}
				reader.close();
			}

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
			FileChannel fc = bfc.getChannel();
			for (PostingList p : postingMap.values()) {
				writePosting(fc, p);
			}
			fc.close();
			bfc.close();
		}

		/* Required: output total number of files. */
		// System.out.println("Total Files Indexed: "+totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1) break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();

			File combfile = new File(outputDirname, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");

			FileChannel fc1 = bf1.getChannel();
			FileChannel fc2 = bf2.getChannel();
			FileChannel fc3 = mf.getChannel();
			PostingList p1 = index.readPosting(fc1);
			PostingList p2 = index.readPosting(fc2);

			// Merge blocks
			while (p1 != null && p2 != null) {
				if (p1.getTermId() == p2.getTermId()) {
					writePosting(fc3, mergePosting(p1, p2));
					p1 = index.readPosting(fc1);
					p2 = index.readPosting(fc2);
				}
				else if (p1.getTermId() < p2.getTermId()) {
					writePosting(fc3, p1);
					p1 = index.readPosting(fc1);
				}
				else {
					writePosting(fc3, p2);
					p2 = index.readPosting(fc2);
				}
			}

			// Add leftover postings
			while (p1 != null) {
				writePosting(fc3, p1);
				p1 = index.readPosting(fc1);
			}
			while (p2 != null) {
				writePosting(fc3, p2);
				p2 = index.readPosting(fc2);
			}

			fc1.close();
			fc2.close();
			fc3.close();
			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(outputDirname, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst() + "\t"
					+ postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();

		return totalFileCount;
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 3) {
			System.err.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
			return;
		}

		/* Get index */
		String className = "";
		try {
			className = args[0];
		}
		catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		String root = args[1];

		/* Get output directory */
		String output = args[2];
		runIndexer(className, root, output);
	}

}
