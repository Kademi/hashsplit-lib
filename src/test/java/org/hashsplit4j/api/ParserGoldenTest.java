package org.hashsplit4j.api;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import junit.framework.TestCase;

/**
 * Pins what the parser produces, so a change to how it gets there cannot change what it gets.
 * <p>
 * The parser had no test at all. Everything downstream is keyed by these hashes - a blob's name is its content, a
 * fanout's name covers the blobs it lists, and the branch hash covers all of it - so any drift here silently
 * invalidates every repository on every server. That makes this the one test worth having before touching the code.
 * <p>
 * The input is generated rather than checked in: an xorshift64 stream, which is reproducible, incompressible enough to
 * exercise the boundary logic, and long enough to produce many blobs and more than one fanout.
 *
 * @author claude
 */
public class ParserGoldenTest extends TestCase {

    /**
     * Deterministic pseudo-random bytes. The multiplier-free xorshift64 of Marsaglia, seeded so the stream never
     * degenerates to zero.
     */
    static byte[] seeded(long seed, int length) {
        if( seed == 0 ) {
            seed = 1;
        }
        byte[] out = new byte[length];
        long x = seed;
        for( int i = 0; i < length; i++ ) {
            x ^= x << 13;
            x ^= x >>> 7;
            x ^= x << 17;
            out[i] = (byte) x;
        }
        return out;
    }

    /**
     * A hash store that records what it was told, in order.
     */
    static class RecordingHashStore implements HashStore {

        final Map<String, List<String>> chunkFanouts = new TreeMap<>();
        final Map<String, List<String>> fileFanouts = new TreeMap<>();
        final List<String> order = new ArrayList<>();

        @Override
        public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
            chunkFanouts.put(hash, new ArrayList<>(blobHashes));
            order.add("cf " + hash + " n=" + blobHashes.size() + " len=" + actualContentLength);
        }

        @Override
        public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
            fileFanouts.put(hash, new ArrayList<>(fanoutHashes));
            order.add("ff " + hash + " n=" + fanoutHashes.size() + " len=" + actualContentLength);
        }

        @Override
        public Fanout getFileFanout(String fileHash) {
            List<String> hashes = fileFanouts.get(fileHash);
            return hashes == null ? null : new FanoutImpl(hashes, 0);
        }

        @Override
        public Fanout getChunkFanout(String fanoutHash) {
            List<String> hashes = chunkFanouts.get(fanoutHash);
            return hashes == null ? null : new FanoutImpl(hashes, 0);
        }

        @Override
        public boolean hasChunk(String fanoutHash) {
            return chunkFanouts.containsKey(fanoutHash);
        }

        @Override
        public boolean hasFile(String fileHash) {
            return fileFanouts.containsKey(fileHash);
        }
    }

    /**
     * A blob store that records sizes and order, but not the bytes - a blob's hash already covers its content.
     */
    static class RecordingBlobStore implements BlobStore {

        final Map<String, Integer> sizes = new TreeMap<>();
        final List<String> order = new ArrayList<>();

        @Override
        public void setBlob(String hash, byte[] bytes) {
            sizes.put(hash, bytes.length);
            order.add("b " + hash + " " + bytes.length);
        }

        @Override
        public byte[] getBlob(String hash) {
            return null;
        }

        @Override
        public boolean hasBlob(String hash) {
            return sizes.containsKey(hash);
        }
    }

    static class Result {

        String fileHash;
        RecordingBlobStore blobs = new RecordingBlobStore();
        RecordingHashStore hashes = new RecordingHashStore();
    }

    static Result parse(byte[] data) throws Exception {
        Result r = new Result();
        r.fileHash = new Parser().parse(new ByteArrayInputStream(data), r.hashes, r.blobs);
        return r;
    }

    /**
     * Every hash the parser emits, for an input long enough to cross many blob boundaries and to hit the size cap.
     */
    public void testGoldenHashes() throws Exception {
        Result r = parse(seeded(88172645463325252L, 4 * 1024 * 1024));

        assertEquals("file hash", "db04c9fbadda4b63e6202238851e84291affc57d", r.fileHash);
        assertEquals("blobs stored", 28, r.blobs.order.size());
        assertEquals("fanouts stored",
                "[cf db04c9fbadda4b63e6202238851e84291affc57d n=28 len=4194304, "
                + "ff db04c9fbadda4b63e6202238851e84291affc57d n=1 len=4194304]",
                r.hashes.order.toString());
    }

    /**
     * The full sequence of blobs, which is the strongest statement of what the parser does: these hashes and these
     * lengths, in this order.
     */
    public void testGoldenBlobSequence() throws Exception {
        Result r = parse(seeded(7, 900_000));

        assertEquals("file hash", "4b6ace331d422690c7ad072645e0bbb9022252c0", r.fileHash);
        String[] expected = {
            "b 5f3c69067e6e85b132de7ec5ab8094b8aa3a628a 500001",
            "b 8e9262c2ad597aac0f19b4ff7351c466b6f51562 11731",
            "b a4b104ee76bfe970a5f70185e9acfcdfd95dec20 51845",
            "b b6f2bda2e4ab382d6702502ecec676e7f1285759 6128",
            "b 72a797f6525451be2d8437d1c43d0fcabb51b0d1 15318",
            "b 4233e730a4a58c88757ebcc2e34276a2c1da0e9f 49708",
            "b ec7db8285a5f594a83eba43fc8db0a1b8bd2c3aa 44935",
            "b 1bd5883c2ab81580cf3ad2c9e8933409fc47963b 13248",
            "b 9999718575611157eaf2d0e9567eb995f4deffc5 1488",
            "b 9ffb4dcc45ed51cd76ed80e8195e89f1622cbc66 70289",
            "b f5f799c7ec51c511d33f5c12ee9d8cf47b92f319 39764",
            "b f85c31ae89a64523f104d390f59372b3e8741c34 24407",
            "b 016396be16e4f2f05e7f5496307e29c16f3b850d 19034",
            "b 0c046df29e6cc06889d5dc3dda143bb385b0aabf 46244",
            "b 526a142256f4837aef98966daf55ecc390a29b68 1810",
            "b df705a0d1b1cd57209875049a604aff83cb2405a 4050",};
        assertEquals("blob count", expected.length, r.blobs.order.size());
        for( int i = 0; i < expected.length; i++ ) {
            assertEquals("blob " + i, expected[i], r.blobs.order.get(i));
        }
    }

    /**
     * A blob is capped at MAX_BLOB_SIZE, so an input with no boundary in it still gets split. A constant stream never
     * fires the rolling boundary - the checksum of all zeroes is zero - so only the cap does the work here.
     * <p>
     * The cap is tested after the byte is appended, which is why a capped blob is 500001 bytes and not 500000. Every
     * stored repository already depends on that off-by-one.
     */
    public void testBlobSizeCap() throws Exception {
        Result r = parse(new byte[1_600_000]);

        assertEquals("file hash", "4a5c3e0295359e751afe100dcf83ec57cf2aeba3", r.fileHash);
        // Identical content gives an identical hash, so the three capped blobs are one object stored three times.
        assertEquals("blobs stored", 4, r.blobs.order.size());
        assertEquals("blob 0", "b c23207e059bb5fa7192dc296eb77d479088c52b9 500001", r.blobs.order.get(0));
        assertEquals("blob 1", "b c23207e059bb5fa7192dc296eb77d479088c52b9 500001", r.blobs.order.get(1));
        assertEquals("blob 2", "b c23207e059bb5fa7192dc296eb77d479088c52b9 500001", r.blobs.order.get(2));
        assertEquals("terminal blob", "b 1acda1b905941e937e6ea9a1a490864aacea669d 99997", r.blobs.order.get(3));
        assertEquals("distinct blobs", 2, r.blobs.sizes.size());
    }

    /**
     * An empty input still produces a blob, a chunk fanout and a file fanout, all named by the double hash of nothing.
     * A zero byte file has to round trip like any other.
     */
    public void testEmptyInput() throws Exception {
        Result r = parse(new byte[0]);

        assertEquals("file hash", "be1bdec0aa74b4dcb079943e70528096cca985f8", r.fileHash);
        assertEquals("blobs stored", 1, r.blobs.order.size());
        assertEquals("blob 0", "b be1bdec0aa74b4dcb079943e70528096cca985f8 0", r.blobs.order.get(0));
        assertEquals("fanouts stored",
                "[cf be1bdec0aa74b4dcb079943e70528096cca985f8 n=1 len=0, "
                + "ff be1bdec0aa74b4dcb079943e70528096cca985f8 n=1 len=0]",
                r.hashes.order.toString());
    }

    /**
     * Reading is buffered in blocks, so a boundary landing exactly on a block edge is the case most likely to break
     * when the loop is restructured. Parsing the same bytes through many different block sizes must give one answer.
     */
    public void testBlockBoundariesDoNotMatter() throws Exception {
        byte[] data = seeded(7, 900_000);
        Result expected = parse(data);

        for( int blockSize : new int[]{1, 2, 3, 127, 128, 129, 1023, 1024, 1025, 65536} ) {
            Result got = new Result();
            got.fileHash = new Parser().parse(new ChunkedStream(data, blockSize), got.hashes, got.blobs);

            assertEquals("file hash at block size " + blockSize, expected.fileHash, got.fileHash);
            assertEquals("blob order at block size " + blockSize, expected.blobs.order, got.blobs.order);
            assertEquals("fanout order at block size " + blockSize, expected.hashes.order, got.hashes.order);
        }
    }

    /**
     * parse(File) wraps the stream in an 8k BufferedInputStream while the parser reads 64k at a time, so the buffer is
     * bypassed and the reads land straight on the file. Same bytes, same answer.
     */
    public void testFileAndStreamAgree() throws Exception {
        byte[] data = seeded(99, 1_400_000);
        Result viaStream = parse(data);

        java.io.File f = java.io.File.createTempFile("parser-golden", ".bin");
        try {
            try (java.io.OutputStream out = new java.io.FileOutputStream(f)) {
                out.write(data);
            }
            RecordingBlobStore blobs = new RecordingBlobStore();
            RecordingHashStore hashes = new RecordingHashStore();
            String fileHash = Parser.parse(f, blobs, hashes);

            assertEquals("file hash", viaStream.fileHash, fileHash);
            assertEquals("blob order", viaStream.blobs.order, blobs.order);
            assertEquals("fanout order", viaStream.hashes.order, hashes.order);
        } finally {
            f.delete();
        }
    }

    /**
     * Hands out at most blockSize bytes per read, so the parser's inner loop sees every alignment.
     */
    static class ChunkedStream extends java.io.InputStream {

        private final byte[] data;
        private final int blockSize;
        private int pos;

        ChunkedStream(byte[] data, int blockSize) {
            this.data = data;
            this.blockSize = blockSize;
        }

        @Override
        public int read() {
            return pos < data.length ? data[pos++] & 0xff : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if( pos >= data.length ) {
                return -1;
            }
            int n = Math.min(Math.min(len, blockSize), data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }
    }
}
