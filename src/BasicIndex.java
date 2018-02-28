
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class BasicIndex implements BaseIndex {

	ByteBuffer bb;

	@Override
	public PostingList readPosting(FileChannel fc) throws IOException {
		// MUST RETURN NULL IF REACHES FILE END
		if (fc.position() >= fc.size()) {
			return null;
		}

		long startingPos = fc.position();
		bb = ByteBuffer.allocate(4);
		fc.read(bb);
		bb.flip();
		PostingList p = new PostingList(bb.getInt());
		fc.position(startingPos + 4);
		bb.clear();
		fc.read(bb);
		bb.flip();
		int freq = bb.getInt();
		fc.position(startingPos + 8);
		bb = ByteBuffer.allocate(freq * 4);
		fc.read(bb);
		bb.flip();
		// System.out.println("Reading:");
		// System.out.println("TermId: " + p.getTermId() + " Freq: " + freq);
		for (int i = 0; i < freq; i++) {
			p.getList().add(bb.getInt());
		}
		// System.out.println(p.getList().toString());
		fc.position(startingPos + ((2 + freq) * 4));
		return p;
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		bb = ByteBuffer.allocate((p.getList().size() + 2) * 4);
		bb.putInt(p.getTermId());
		bb.putInt(p.getList().size());
		for (Integer docId : p.getList()) {
			bb.putInt(docId);
		}
		bb.flip();
		// System.out.println("ByteBuffer written: " + Arrays.toString(bb.array()));
		try {
			fc.write(bb);
			// System.out.println("Successfully Written!");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
