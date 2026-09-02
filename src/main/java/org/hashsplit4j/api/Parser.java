package org.hashsplit4j.api;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA384Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The parser will take a stream of bytes and split it into chunks with an
 * average size of MASK bytes (eg 8k for BUP, 64k for us). The chunk boundaries
 * are determined by looking at a rolling checksum of the last 128 bytes, when
 * the lowest 13 bits of this checksum we take that as a boundary.
 *
 * This algorithm results in boundaries which are fairly stable with file
 * modifications, so that if a previously chunked file is modified, most of the
 * chunks should still match the new file.
 *
 * The main method to call is parse, and output information is given to the
 * provided HashStore
 *
 * @author brad
 */
public class Parser {

    private static final Logger log = LoggerFactory.getLogger(Parser.class);

    //private static final int MASK = 0xFF; // avg size 256bytes
    //private static final int MASK = 0xFFF; // avg size 4k
    //private static final int MASK = 0x1FFF; // avg size 22k ... 8191
    //private static final int MASK = 0x3FFF; // avg size 19k
    //private static final int MASK = 0xFFFF;  // average blob size of 64k (well, should be. but seeing 15k for vids?)
    private static final int MASK = 0xFFFFF;
    private static final int FANOUT_MASK = 0x7FFFFFF; // about 1024 hashes per fanout

    //private static final Integer MAX_BLOB_SIZE = null; // disable max blob size
    private static final Integer MAX_BLOB_SIZE = 500000; // max of 500k

    /**
     * Bytes read from the stream at a time. This was 1024, which cost an outer loop iteration per kilobyte for no
     * benefit; the block size does not affect the output, which ParserGoldenTest checks across ten of them.
     */
    private static final int READ_BUFFER_SIZE = 64 * 1024;

    /**
     * Starting size of the blob accumulator. The median blob is tens of kilobytes against a 500k cap, so starting at
     * the cap would waste most of it; this grows by doubling and is reused for every blob after the first few.
     */
    private static final int INITIAL_BLOB_BUFFER_SIZE = 64 * 1024;

    public static String parse(File f, BlobStore blobStore, HashStore hashStore) throws FileNotFoundException, IOException {
        Parser parser = new Parser();
        FileInputStream fin = null;
        BufferedInputStream bufIn = null;
        try {
            fin = new FileInputStream(f);
            bufIn = new BufferedInputStream(fin);
            return parser.parse(bufIn, hashStore, blobStore);
        } finally {
            IOUtils.closeQuietly(bufIn);
            IOUtils.closeQuietly(fin);
        }
    }

    private final String algorithmName;
    private boolean cancelled;
    private long numBytes;

    public Parser() {
        this.algorithmName = "SHA1";
    }

    public Parser(String algorithmName) {
        this.algorithmName = algorithmName;
    }

    /**
     * Returns a hex enccoded SHA1 hash of the whole file. This can be used to
     * locate the files bytes again
     *
     * @param in
     * @param hashStore
     * @param blobStore
     * @return HEX encoded hash string
     * @throws IOException
     */
    public String parse(InputStream in, HashStore hashStore, BlobStore blobStore) throws IOException {
        return parse(in, hashStore, blobStore, null);
    }

    public String parse(InputStream in, HashStore hashStore, BlobStore blobStore, Consumer<Long> callback) throws IOException {

//        if (log.isInfoEnabled()) {
//            log.info("parse. inputstream: " + in);
//        }
        Rsum rsum = new Rsum(128);
        int numBlobs = 0;
        byte[] arr = new byte[READ_BUFFER_SIZE];

        // The blob being accumulated. A plain array rather than a ByteArrayOutputStream: every method on that class is
        // synchronized, and the loop below used to call write(int) and size() once per byte, so a 6.5GB file paid for
        // 13 billion uncontended monitor operations. One buffer, reused for every blob, so this allocates nothing per
        // blob beyond the exact-sized copy setBlob needs - which is what the old toByteArray did too.
        byte[] blobBuf = new byte[INITIAL_BLOB_BUFFER_SIZE];
        int blobLen = 0;

        // MAX_BLOB_SIZE is an Integer so it can be null to disable the cap. Unboxed once here, because it is consulted
        // on every pass, and Integer.MAX_VALUE makes the "no cap" case fall out of the same arithmetic.
        final int maxBlob = MAX_BLOB_SIZE == null ? Integer.MAX_VALUE : MAX_BLOB_SIZE;

        List<String> blobHashes = new ArrayList<>();

        // MessageDigest rather than BouncyCastle, for these three only. HotSpot has a SHA-1 intrinsic - SHA-NI on any
        // recent x86 - and it exists only for the JDK provider: measured on this machine, bulk updates run at
        // 2581 MB/s against BouncyCastle's 460 MB/s. Three digests cover every byte of input, so BouncyCastle alone
        // caps the parser at about 150 MB/s no matter what else is fixed.
        //
        // The hashes are unaffected: SHA-1 is SHA-1, and toHex's outer hash was always DigestUtils, ie the JDK.
        // getCrypt and the public toHex(Digest) keep their BouncyCastle types, because Crypt, HashCalc, FileBlobStore
        // and kademi-dev's BlobEmailContentService all call them.
        MessageDigest blobCrc = getMessageDigest(algorithmName);
        MessageDigest fanoutCrc = getMessageDigest(algorithmName);
        MessageDigest fileCrc = getMessageDigest(algorithmName);

        long fanoutLength = 0;
        long fileLength = 0;

        int s = in.read(arr, 0, arr.length);
        if( log.isTraceEnabled() ) {
            log.trace("initial block size: " + s);
        }

        List<String> fanoutHashes = new ArrayList<>();
        while( s >= 0 ) {
            numBytes += s;
            //log.trace("numBytes: {}", numBytes);
            if( cancelled ) {
                throw new IOException("operation cancelled");
            }
            // Walk the block a run at a time rather than a byte at a time. Each run ends where the next blob does,
            // and everything that used to be per-byte - three digest updates and the copy into the blob buffer - is
            // done once for the whole run. The digests are the reason this matters: a per-byte update is a virtual call
            // into an internal one-byte buffer, where a bulk update feeds the compression function whole blocks.
            int i = 0;
            while( i < s ) {
                // How many bytes may be appended before the cap fires. The cap is tested after appending, so the byte
                // that takes a blob past MAX_BLOB_SIZE is the last byte in it - which is why capped blobs are 500001
                // bytes and not 500000, and why this is maxBlob + 1. Every stored repository depends on that.
                int room = maxBlob + 1 - blobLen;
                int run = Math.min(s - i, room);

                int consumed = rsum.rollBoundary(arr, i, run, MASK);
                boolean boundary = consumed >= 0;
                int n = boundary ? consumed : run;

                blobCrc.update(arr, i, n);
                fanoutCrc.update(arr, i, n);
                fileCrc.update(arr, i, n);

                if( blobLen + n > blobBuf.length ) {
                    blobBuf = grow(blobBuf, blobLen + n, maxBlob);
                }
                System.arraycopy(arr, i, blobBuf, blobLen, n);
                blobLen += n;

                fanoutLength += n;
                fileLength += n;
                i += n;

                boolean limited = blobLen > maxBlob;
                if( limited ) {
                    log.warn("HIT BLOB LIMIT: " + blobLen);
                }
                if( !boundary && !limited ) {
                    continue;
                }

                // The checksum of the last byte rolled, which is the value the old loop called x.
                int x = rsum.getValue();

                String blobCrcHex = toHex(blobCrc);
                byte[] blobBytes = Arrays.copyOf(blobBuf, blobLen);
                if( log.isInfoEnabled() ) {
                    log.info("Store blob: " + blobCrcHex + " length=" + blobBytes.length + " hash: " + x + " mask: " + MASK);
                }

                if( callback != null ) {
                    callback.accept(numBytes);
                }

                blobStore.setBlob(blobCrcHex, blobBytes);

                blobLen = 0;
                blobHashes.add(blobCrcHex);
                blobCrc.reset();
                if( (x & FANOUT_MASK) == FANOUT_MASK ) {
                    String fanoutCrcVal = toHex(fanoutCrc);
                    fanoutHashes.add(fanoutCrcVal);
                    //log.info("set chunk fanout: {} length={}", fanoutCrcVal, fanoutLength);
                    hashStore.setChunkFanout(fanoutCrcVal, blobHashes, fanoutLength);
                    fanoutLength = 0;
                    fanoutCrc.reset();
                    blobHashes = new ArrayList<>();
                }
                numBlobs++;
                rsum.reset();
            }

            s = in.read(arr, 0, arr.length);
        }
        // Need to store terminal data, ie data which has been accumulated since the last boundary
        String blobCrcHex = toHex(blobCrc);
        //System.out.println("Store terminal blob: " + blobCrcHex);

        if( callback != null ) {
            callback.accept(numBytes);
        }

        blobStore.setBlob(blobCrcHex, Arrays.copyOf(blobBuf, blobLen));
        numBlobs++;
        blobHashes.add(blobCrcHex);
        String fanoutCrcVal = toHex(fanoutCrc);
        //log.info("set terminal chunk fanout: {} length={}" ,fanoutCrcVal, fanoutLength);

        hashStore.setChunkFanout(fanoutCrcVal, blobHashes, fanoutLength);
        fanoutHashes.add(fanoutCrcVal);

        // Now store a fanout for the whole file. The contained hashes locate other fanouts
        String fileCrcVal = toHex(fileCrc);
//        if (log.isInfoEnabled()) {
//            log.info("set file fanout: " + fanoutCrcVal + "  length=" + fileLength + " avg blob size=" + fileLength / numBlobs);
//        }
        hashStore.setFileFanout(fileCrcVal, fanoutHashes, fileLength);
        return fileCrcVal;
    }

    /**
     * Doubles a buffer until it holds needed bytes, never overshooting the cap by more than one byte.
     */
    private static byte[] grow(byte[] buf, int needed, int maxBlob) {
        int size = buf.length;
        while( size < needed ) {
            size *= 2;
            if( size < 0 || size > maxBlob ) { // the cap is exclusive, so one more byte can land in a blob
                size = maxBlob == Integer.MAX_VALUE ? needed : maxBlob + 1;
                break;
            }
        }
        return Arrays.copyOf(buf, Math.max(size, needed));
    }

    /**
     * The JDK digest matching what {@link #getCrypt(String)} would return, for the parser's own use.
     *
     * @param algorithmName as accepted by getCrypt; anything unrecognised means SHA-1
     * @return a fresh MessageDigest
     */
    private static MessageDigest getMessageDigest(String algorithmName) {
        String jdkName = "SHA-1";
        if( StringUtils.isNotEmpty(algorithmName) ) {
            String cleaned = StringUtils.trim(StringUtils.upperCase(algorithmName)).replaceAll("[^A-Za-z0-9]", "");
            switch( cleaned ) {
                case "SHA256":
                    jdkName = "SHA-256";
                    break;
                case "SHA384":
                    jdkName = "SHA-384";
                    break;
                case "SHA512":
                    jdkName = "SHA-512";
                    break;
                default:
                    jdkName = "SHA-1";
                    break;
            }
        }
        try {
            return MessageDigest.getInstance(jdkName);
        } catch( NoSuchAlgorithmException e ) {
            // Every one of these is required of a conformant JRE, so this cannot happen.
            throw new IllegalStateException("No " + jdkName + " available", e);
        }
    }

    /**
     * The double hash, for a JDK digest. Same shape as {@link #toHex(Digest)}: finish the digest, then hex the hash of
     * its raw bytes. digest() resets, as BouncyCastle's doFinal does.
     *
     * @param crypt digest to finish
     * @return HEX encoded hash of the digest's own bytes
     */
    private static String toHex(MessageDigest crypt) {
        byte[] result = crypt.digest();
        switch( crypt.getAlgorithm() ) {
            case "SHA-256":
                return DigestUtils.sha256Hex(result);
            case "SHA-384":
                return DigestUtils.sha384Hex(result);
            case "SHA-512":
                return DigestUtils.sha512Hex(result);
            default:
                return DigestUtils.sha1Hex(result);
        }
    }

    public static Digest getCrypt(String algorithmName) {
        if( StringUtils.isEmpty(algorithmName) ) {
            return new SHA1Digest();
        }

        algorithmName = StringUtils.trim(StringUtils.upperCase(algorithmName));
        algorithmName = algorithmName.replaceAll("[^A-Za-z0-9]", "");

        switch( algorithmName ) {
            case "SHA256":
                return new SHA256Digest();
            case "SHA384":
                return new SHA384Digest();
            case "SHA512":
                return new SHA512Digest();
            default:
                return new SHA1Digest();
        }
    }

    public static Digest getCrypt() {
        return getCrypt(null);
    }

    public static String toHex(Digest crypt) {
        byte[] result = new byte[crypt.getDigestSize()];
        crypt.doFinal(result, 0);

        String an = crypt.getAlgorithmName();
        switch( an ) {
            case "SHA-1":
                return DigestUtils.sha1Hex(result);
            case "SHA-256":
                return DigestUtils.sha256Hex(result);
            case "SHA-384":
                return DigestUtils.sha384Hex(result);
            case "SHA-512":
                return DigestUtils.sha512Hex(result);
            default:
                return DigestUtils.sha1Hex(result);
        }
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public long getNumBytes() {
        return numBytes;
    }

}
