
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Query {

	// Term id -> position in index file
	private Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private BaseIndex index = null;

	// indicate whether the query service is running or not
	private boolean running = false;
	private RandomAccessFile indexFile = null;

	/*
	 * Read a posting list with a given termID from the file You should seek to the file position of this specific posting list and read it back.
	 */
	private PostingList readPosting(FileChannel fc, int termId) throws IOException {
		fc.position(posDict.get(termId));
		return index.readPosting(fc);
	}

	public void runQueryService(String indexMode, String indexDirname) throws IOException {
		// Get the index reader
		try {
			Class<?> indexClass = Class.forName(indexMode + "Index");
			index = (BaseIndex) indexClass.newInstance();
		}
		catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		// Get Index file
		File inputdir = new File(indexDirname);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + indexDirname);
			return;
		}

		/* Index file */
		indexFile = new RandomAccessFile(new File(indexDirname, "corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(indexDirname, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(indexDirname, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(indexDirname, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[2]));
		}
		postReader.close();

		this.running = true;
	}

	public List<Integer> retrieve(String query) throws IOException {
		if (!running) {
			System.err.println("Error: Query service must be initiated");
		}
		List<Integer> matchedDocIds = new ArrayList<Integer>();
		List<PostingList> postings = new ArrayList<PostingList>();
		FileChannel fc = indexFile.getChannel();
		boolean docIdInAllPostings;

		// Split query into words
		String[] tokens = query.trim().split("\\s+");

		// Add term id of each query to the list
		for (String token : tokens) {
			if (termDict.get(token) == null) {
				return null;
			}
			postings.add(readPosting(fc, termDict.get(token)));
		}

		// Putting the smallest posting list at the front
		Collections.sort(postings, new Comparator<PostingList>() {
			public int compare(PostingList p1, PostingList p2) {
				return Integer.compare(p1.getList().size(), p2.getList().size());
			}
		});

		// Add doc id that is contained in every posting to the output list
		for (int docId : postings.get(0).getList()) {
			docIdInAllPostings = true;
			for (PostingList p : postings) {
				// Skip the doc id when it's not inside every posting
				if (!p.getList().contains(docId)) {
					docIdInAllPostings = false;
					break;
				}
			}
			if (docIdInAllPostings) {
				matchedDocIds.add(docId);
			}
		}
		return matchedDocIds;

	}

	String outputQueryResult(List<Integer> res) {
		StringBuilder output = new StringBuilder();
		ArrayList<String> docNames = new ArrayList<String>();

		// No doc id found from the query
		if (res == null) {
			output.append("no results found\n");
			System.out.println(output);
			return output.toString();
		}
		else if (res.isEmpty()) {
			output.append("no results found\n");
			System.out.println(output);
			return output.toString();
		}

		// Get all the doc names
		for (int docId : res) {
			docNames.add(docDict.get(docId));
		}

		docNames.sort(null);
		for (String docName : docNames) {
			System.out.println(docName);
			output.append(docName + "\n");
		}
		System.out.println();
		return output.toString();
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
			return;
		}

		/* Get index */
		String className = null;
		try {
			className = args[0];
		}
		catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get index directory */
		String input = args[1];

		Query queryService = new Query();
		queryService.runQueryService(className, input);

		/* Processing queries */
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/* For each query */
		String line = null;
		while ((line = br.readLine()) != null) {
			List<Integer> hitDocs = queryService.retrieve(line);
			queryService.outputQueryResult(hitDocs);
		}

		br.close();
	}

	protected void finalize() {
		try {
			if (indexFile != null) indexFile.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
