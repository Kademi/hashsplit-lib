package org.hashsplit4j.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just class to save byte[] to a file with a key for efficient lookup
 *
 * @author brad
 */
public class SimpleFileDb {

    private static final Logger log = LoggerFactory.getLogger(SimpleFileDb.class);

    private final String name;
    private final File keysFile;
    private final File valuesFile;

    private final Map<String, DbItem> mapOfItems = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean enabled = false;

    /**
     *
     * @param name - just an identifier for this instance
     * @param keysFile
     * @param valuesFile
     */
    public SimpleFileDb(String name, File keysFile, File valuesFile) {
        this.name = name;
        this.keysFile = keysFile;
        this.valuesFile = valuesFile;
    }

    public String getKeysFilePath() {
        try {
            return keysFile.getCanonicalPath();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getValuesFilePath() {
        try {
            return valuesFile.getCanonicalPath();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public long getKeysFileSize() {
        return keysFile.length();
    }

    public long getValuesFileSize() {
        return valuesFile.length();
    }

    public String getName() {
        return name;
    }

    public void replaceData(File newKeysFile, File newValsFile) {
        lock.writeLock().lock();
        try {
            enabled = false;
            mapOfItems.clear();
            replaceFileContent(newKeysFile, keysFile);
            replaceFileContent(newValsFile, valuesFile);
            initLocked();
        } catch (IOException ex) {
            throw new RuntimeException("Couldnt initialise SimpleDb", ex);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void init() throws FileNotFoundException, IOException {
        lock.writeLock().lock();
        try {
            initLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // must only be called while holding the write lock
    private void initLocked() throws FileNotFoundException, IOException {
        if (keysFile.exists()) {
            log.warn("init: using keysfile {}", keysFile.getAbsolutePath() );
            try (FileInputStream fin = new FileInputStream(keysFile)) {
                InputStreamReader r1 = new InputStreamReader(fin);
                BufferedReader reader = new BufferedReader(r1);
                String line = reader.readLine();
                while (line != null) {
                    parseAndAdd(line);
                    line = reader.readLine();
                }
            }
        } else {
            log.warn("init: keysfile does not exist: {}", keysFile.getAbsolutePath() );
        }
        enabled = true;
    }

    public boolean isEnabled() {
        lock.readLock().lock();
        try {
            return enabled;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return mapOfItems.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean contains(String hash) {
        lock.readLock().lock();
        try {
            return mapOfItems.containsKey(hash);
        } finally {
            lock.readLock().unlock();
        }
    }

    public DbItem put(String key, byte[] val) throws FileNotFoundException, IOException {
        lock.writeLock().lock();
        try {
            if (mapOfItems.containsKey(key)) {
                throw new RuntimeException("Key " + key + " is already present");
            }

            long startPos;
            long finishPos;
            try (FileOutputStream fout = new FileOutputStream(valuesFile, true)) { //open in append mode
                FileChannel chan = fout.getChannel();
                startPos = chan.position();
                ByteBuffer bb = ByteBuffer.wrap(val);
                chan.write(bb);
                finishPos = chan.position();
            }
            long length = finishPos - startPos;
            if (length != val.length) {
                throw new RuntimeException("Inserting blob into simplefiledb failed, lengths differ. Should be " + val.length + " but is " + length + ", for key=" + key);
            }
            DbItem dbItem = new DbItem(startPos, finishPos);

            log.info("put: start={} finish={} key={}", startPos, finishPos, key);
            try (FileOutputStream fout = new FileOutputStream(keysFile, true)) { //open in append mode
                try (FileChannel chan = fout.getChannel()) {
                    String line = key + "," + startPos + "," + finishPos + "\n"; // use text for ease of troubleshooting
                    ByteBuffer bb = ByteBuffer.wrap(line.getBytes());
                    chan.write(bb);
                }
            }

            mapOfItems.put(key, dbItem);

            return dbItem;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public byte[] get(String key) throws FileNotFoundException, IOException {
        lock.readLock().lock();
        try {
            DbItem item = mapOfItems.get(key);
            if (item == null) {
                return null;
            }
            return getLocked(item);
        } finally {
            lock.readLock().unlock();
        }
    }

    public byte[] get(DbItem item) throws FileNotFoundException, IOException {
        lock.readLock().lock();
        try {
            return getLocked(item);
        } finally {
            lock.readLock().unlock();
        }
    }

    // must only be called while holding either lock
    private byte[] getLocked(DbItem item) throws FileNotFoundException, IOException {
        RandomAccessFile raf = new RandomAccessFile(valuesFile, "r");
        try (FileChannel chan = raf.getChannel()) {
            int size = (int) (item.finish - item.start);
            ByteBuffer bb = ByteBuffer.allocate(size);
            chan.position(item.start).read(bb);
            bb.flip();
            byte[] arr = bb.array();
            return arr;
        }
    }

    private void parseAndAdd(String line) {
        String[] arr = line.split(",");
        if (arr.length != 3) {
            log.info("Invalid line, not 3 parts: {}", line);
        } else {
            String key = arr[0];
            long start = Long.parseLong(arr[1]);
            long finish = Long.parseLong(arr[2]);
            DbItem dbItem = new DbItem(start, finish);
            mapOfItems.put(key, dbItem);
        }
    }

    private void replaceFileContent(File source, File dest) throws FileNotFoundException, IOException {
        log.info("replaceFileContent: replacing content of dest file {} with source file {}", dest.getAbsolutePath(), source.getAbsolutePath());
        try( FileInputStream newFileIn = new FileInputStream(source)) {
            try( FileOutputStream fileOut = new FileOutputStream(dest, false)) {
                IOUtils.copyLarge(newFileIn, fileOut);
            }
        }
    }

    public class DbItem {

        private final long start;
        private final long finish;

        public DbItem(long start, long finish) {
            this.start = start;
            this.finish = finish;
        }

        byte[] data() throws IOException {
            return SimpleFileDb.this.get(this);
        }
    }
}
