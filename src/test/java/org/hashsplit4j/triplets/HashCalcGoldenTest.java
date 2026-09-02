package org.hashsplit4j.triplets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import junit.framework.TestCase;

/**
 * Pins what a directory hash is, so a change to how it is computed cannot change what
 * it comes out as. Every repository on every server is keyed by these values.
 *
 * @author claude
 */
public class HashCalcGoldenTest extends TestCase {

    static class T implements ITriplet {

        private final String name, hash, type;

        T(String name, String hash, String type) {
            this.name = name;
            this.hash = hash;
            this.type = type;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getHash() {
            return hash;
        }

        @Override
        public String getType() {
            return type;
        }
    }

    private final HashCalc hashCalc = HashCalc.getInstance();

    /**
     * Captured from the unmodified code before any of it was touched.
     */
    public void testGoldenHashes() throws Exception {
        List<ITriplet> ascii = Arrays.<ITriplet>asList(
                new T("apple.txt", "0f1e2d3c4b5a69788796a5b4c3d2e1f001234567", "f"),
                new T("sub", "1111111111111111111111111111111111111111", "d"),
                new T("zebra.txt", "2222222222222222222222222222222222222222", "f"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals("d7ca5048512548e5863b0b88004f45268d3e9c81", hashCalc.calcHash(ascii, out));
        assertEquals(153, out.size());

        assertEquals("an empty directory", "be1bdec0aa74b4dcb079943e70528096cca985f8",
                hashCalc.calcHash(new ArrayList<ITriplet>(), new ByteArrayOutputStream()));
    }

    /**
     * The listing written to the stream and the hash returned must describe the same
     * bytes, or a caller ends up storing a blob under a name that does not match it.
     */
    public void testWrittenBytesMatchTheHash() throws Exception {
        List<ITriplet> triplets = Arrays.<ITriplet>asList(
                new T("a.txt", "1111111111111111111111111111111111111111", "f"),
                new T("b.txt", "2222222222222222222222222222222222222222", "f"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String hash = hashCalc.calcHash(triplets, out);

        assertEquals("a.txt:1111111111111111111111111111111111111111:f\n"
                + "b.txt:2222222222222222222222222222222222222222:f\n",
                new String(out.toByteArray(), StandardCharsets.UTF_8));
        // Hashing the written bytes through the other entry point must agree.
        assertEquals(hash, hashCalc.calcHash(
                hashCalc.parseTriplets(new ByteArrayInputStream(out.toByteArray()))));
    }

    /**
     * Names are encoded as UTF-8 rather than in the platform default charset.
     * <p>
     * The golden below is what a UTF-8 JVM always produced, so on such a JVM this test
     * cannot fail - it is here for the ones where file.encoding is something else,
     * where the old code produced a7e2ef995cb43cc680db531558b4cbd4e1c41721 for the same
     * two files. A hash that depends on the JVM's locale is not a hash of the content.
     */
    public void testNonAsciiNamesAreUtf8() throws Exception {
        List<ITriplet> triplets = Arrays.<ITriplet>asList(
                new T("café.txt", "3333333333333333333333333333333333333333", "f"),
                new T("日本語.txt", "4444444444444444444444444444444444444444", "f"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertEquals("c0104cb137b62c4fc7c2f8189a83cf3ab5421fb2", hashCalc.calcHash(triplets, out));
        assertEquals("the listing is not UTF-8",
                "café.txt:3333333333333333333333333333333333333333:f\n"
                + "日本語.txt:4444444444444444444444444444444444444444:f\n",
                new String(out.toByteArray(), StandardCharsets.UTF_8));
    }

    /**
     * A colon is legal in a file name on Linux and macOS. Splitting on every colon and
     * taking elements 0, 1 and 2 turned "a:b.txt:&lt;hash&gt;:f" into name "a", hash
     * "b.txt" and type "&lt;hash&gt;", so the listing could not round trip. The hash is
     * hex and the type is one letter, so counting back from the end is unambiguous.
     */
    public void testNameContainingAColonRoundTrips() throws Exception {
        List<ITriplet> triplets = Arrays.<ITriplet>asList(
                new T("a:b.txt", "5555555555555555555555555555555555555555", "f"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String hash = hashCalc.calcHash(triplets, out);
        assertEquals("161eb3ae63404d8f9768ce365c4641dda5c9998f", hash);

        List<ITriplet> back = hashCalc.parseTriplets(new ByteArrayInputStream(out.toByteArray()));
        assertEquals(1, back.size());
        assertEquals("a:b.txt", back.get(0).getName());
        assertEquals("5555555555555555555555555555555555555555", back.get(0).getHash());
        assertEquals("f", back.get(0).getType());
        // And re-hashing what was parsed gives the same answer.
        assertEquals(hash, hashCalc.calcHash(back));
    }

    public void testSortOrdersByNameThenType() throws Exception {
        List<ITriplet> triplets = new ArrayList<ITriplet>(Arrays.<ITriplet>asList(
                new T("zebra", "1111111111111111111111111111111111111111", "f"),
                new T("apple", "2222222222222222222222222222222222222222", "f"),
                new T("apple", "3333333333333333333333333333333333333333", "d"),
                new T("mango", "4444444444444444444444444444444444444444", "d")));
        hashCalc.sort(triplets);

        List<String> got = new ArrayList<String>();
        for (ITriplet t : triplets) {
            got.add(t.getName() + ":" + t.getType());
        }
        assertEquals(Arrays.asList("apple:d", "apple:f", "mango:d", "zebra:f"), got);
    }

    /**
     * The comparator guarded its first argument only, so compare(a, null) threw while
     * compare(null, a) returned -1. That breaks the Comparator contract, and whether it
     * threw depended on the order the sort happened to compare a pair in.
     */
    public void testComparatorHandlesNullsSymmetrically() throws Exception {
        HashCalc.ITripletComparator comparator = new HashCalc.ITripletComparator();
        ITriplet a = new T("a", "1111111111111111111111111111111111111111", "f");

        assertEquals(0, comparator.compare(null, null));
        assertTrue(comparator.compare(null, a) < 0);
        assertTrue("compare(a, null) must not throw", comparator.compare(a, null) > 0);

        // And a list containing a null sorts rather than throwing, whatever the order.
        List<ITriplet> withNull = new ArrayList<ITriplet>(Arrays.<ITriplet>asList(a, null));
        hashCalc.sort(withNull);
        assertNull(withNull.get(0));
        Collections.reverse(withNull);
        hashCalc.sort(withNull);
        assertNull(withNull.get(0));
    }
}
