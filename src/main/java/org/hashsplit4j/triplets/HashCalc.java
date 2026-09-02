package org.hashsplit4j.triplets;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.bouncycastle.crypto.Digest;
import org.hashsplit4j.api.Parser;
import org.hashsplit4j.store.NullBlobStore;
import org.hashsplit4j.store.NullHashStore;

/**
 *
 * @author brad
 */
public class HashCalc {

    private static final HashCalc hashCalc = new HashCalc();
    private static final ITripletComparator COMPARATOR = new ITripletComparator();

    public static HashCalc getInstance() {
        return hashCalc;
    }

    /**
     * Calculates the directory hash of the given members
     *
     * @param childDirEntries
     * @return calculated hash
     * @throws IOException
     */
    public String calcHash(Iterable<? extends ITriplet> childDirEntries) throws IOException {
        OutputStream nulOut = new NullOutputStream();
        return calcHash(childDirEntries, nulOut);
    }

    /**
     * Calculates the hash of the given triplets (ie the directory hash with the
     * given members), and writes the standard format for the triplets to the
     * given output stream
     *
     * @param childDirEntries
     * @param out
     * @return calculated hash
     * @throws IOException
     */
    public String calcHash(Iterable<? extends ITriplet> childDirEntries, OutputStream out) throws IOException {
        MessageDigest digest = sha1();
        for (ITriplet r : childDirEntries) {
            // UTF-8 explicitly. String.getBytes() uses the platform default charset,
            // so a directory holding a non-ASCII name hashed differently depending on
            // the JVM's file.encoding - the hash was not a function of the content.
            // Encoded once and used for both the digest and the output, so the two
            // cannot disagree.
            byte[] line = toHashableText(r.getName(), r.getHash(), r.getType()).getBytes(StandardCharsets.UTF_8);
            digest.update(line, 0, line.length);
            out.write(line);
        }
        return DigestUtils.sha1Hex(digest.digest());
    }

    /**
     * SHA-1 from the JDK provider rather than BouncyCastle, which is ~5x faster for
     * the same bytes. The value is unchanged: the hash of a directory is
     * sha1Hex(sha1(lines)), and only the inner digest is computed here.
     */
    private static MessageDigest sha1() {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 is required and not available", e);
        }
    }

    /**
     *
     * @param name - the name of the resource as it appears withint the current
     * directory
     * @param crc - the hash of the resource. Either the crc of the file, of the
     * hashed value of its members if a directory (ie calculated with this
     * method)
     * @param type - "f" = file, "d" = directory
     * @return Hashable Text 
     */
    public static String toHashableText(String name, String crc, String type) {
        String line = name + ":" + crc + ":" + type + '\n';
        return line;
    }

    /**
     * @deprecated calcHash no longer uses this. Kept because it is public API.
     */
    @Deprecated
    public static void appendLine(String line, Digest cout) {
        if (line == null) {
            return;
        }
        // One update for the whole line, not one per byte, and UTF-8 rather than the
        // platform default.
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        cout.update(bytes, 0, bytes.length);
    }

    public List<ITriplet> parseTriplets(InputStream in) throws IOException {
        // UTF-8, to match what calcHash writes.
        Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
        BufferedReader bufIn = new BufferedReader(reader);
        List<ITriplet> list = new ArrayList<ITriplet>();
        String line = bufIn.readLine();
        while (line != null) {
            Triplet triplet = parse(line);
            list.add(triplet);
            line = bufIn.readLine();
        }
        return list;
    }

    private Triplet parse(String line) {
        try {
            // Counted from the right, because a file name may contain a colon while a
            // hash (hex) and a type (one letter) cannot. Splitting on every colon and
            // taking 0, 1, 2 turned "a:b.txt:<hash>:f" into name "a", hash "b.txt" and
            // type "<hash>", so such a listing could not round trip.
            int lastColon = line.lastIndexOf(':');
            int prevColon = line.lastIndexOf(':', lastColon - 1);
            if (lastColon < 0 || prevColon < 0) {
                throw new IllegalArgumentException("expected name:hash:type");
            }
            Triplet triplet = new Triplet();
            triplet.setName(line.substring(0, prevColon));
            triplet.setHash(line.substring(prevColon + 1, lastColon));
            triplet.setType(line.substring(lastColon + 1));
            return triplet;
        } catch (Throwable e) {
            throw new RuntimeException("Couldnt parse - " + line, e);
        }
    }

    public Map<String, ITriplet> toMap(List<ITriplet> triplets) {
        Map<String, ITriplet> map = new HashMap<>();
        for (ITriplet t : triplets) {
            map.put(t.getName(), t);
        }
        return map;
    }

    public void verifyHash(File f, String expectedHash) throws IOException {
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(f);
            verifyHash(fin, expectedHash);
        } finally {
            IOUtils.closeQuietly(fin);
        }
    }

    public void verifyHash(InputStream fin, String expectedHash) throws IOException {
        BufferedInputStream bufIn = null;
        try {
            bufIn = new BufferedInputStream(fin);
            Parser parser = new Parser();
            NullBlobStore blobStore = new NullBlobStore();
            NullHashStore hashStore = new NullHashStore();
            String actualHash = parser.parse(bufIn, hashStore, blobStore);
            if (!actualHash.equals(expectedHash)) {
                throw new IOException("File does not have the expected hash value: Expected: " + expectedHash + " actual:" + actualHash);
            }
        } finally {
            IOUtils.closeQuietly(bufIn);
        }
    }

    public void sort(List<? extends ITriplet> list) {
        Collections.sort(list, COMPARATOR);
    }

    /**
     * Just reads a hex formatted SHA1 hash from the inputstream. Assumes that
     * the string is the first line
     *
     * @param in
     * @return hash
     * @throws IOException
     */
    public String readHash(InputStream in) throws IOException {
        InputStreamReader r = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(r);
        String hash = br.readLine();
        return hash;
    }

    public void writeHash(String hash, OutputStream out) throws IOException {
        OutputStreamWriter w = new OutputStreamWriter(out);
        BufferedWriter bw = new BufferedWriter(w);
        bw.write(hash);
        bw.newLine();
        bw.flush();
        w.flush();
        out.flush();
    }

    public static class ITripletComparator implements Comparator<ITriplet> {

        @Override
        public int compare(ITriplet o1, ITriplet o2) {
            if (o1 == null || o2 == null) {
                // Symmetric: the old code guarded o1 only, so compare(a, null) threw
                // rather than ordering, which breaks the Comparator contract and threw
                // or not depending on the order TimSort happened to compare a pair.
                return o1 == o2 ? 0 : (o1 == null ? -1 : 1);
            } else {
                if (o1.getName().equals(o2.getName())) {
                    // name is equal, so differentiate on type
                    return o1.getType().compareTo(o2.getType());
                } else {
                    return o1.getName().compareTo(o2.getName());
                }
            }
        }
    }
}
