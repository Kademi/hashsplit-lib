package org.hashsplit4j.store;

import org.hashsplit4j.api.ReceivingBlobStore;
import org.hashsplit4j.api.PushingBlobStore;
import io.milton.event.EventManager;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.BooleanUtils;
import org.hashsplit4j.api.BlobImpl;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.utils.FileUtil;
import org.hashsplit4j.event.NewFileBlobEvent;
import org.hashsplit4j.utils.FileSystem2Utils;

/**
 * Stores blobs straight into a file system
 *
 * @author brad
 */
public class FileSystem2BlobStore implements BlobStore, PushingBlobStore, ReceivingBlobStore {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FileSystem2BlobStore.class);
    private static final String TMP_SUFFIX = ".tmp";

    private final File root;
    private final EventManager eventManager;
    private final BlockingQueue<BlobImpl> queue = new ArrayBlockingQueue<>(1000);
    private ReceivingBlobStore receivingBlobStore;

    private final ExecutorService processor = Executors.newFixedThreadPool(5);
    private final TimerTask processQueue = new ProcessQueue();
    private final Timer timer = new Timer();

    private Boolean setReadable;
    private Boolean setReadableOwnerOnly;

    private Future<?> fsScanner;

    public FileSystem2BlobStore(File root) {
        this.root = root;
        this.eventManager = null;
        timer.schedule(processQueue, 1000);
    }

    public FileSystem2BlobStore(File root, EventManager eventManager) throws IOException {
        this.root = root;
        this.eventManager = eventManager;
        timer.schedule(processQueue, 1000);
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        setBlob(hash, bytes, true);
    }

    public void setBlob(String hash, byte[] bytes, boolean enableEvent) {
        File blob = FileSystem2Utils.toFile(root, hash);
        if (bytes.length > 0 ? blob.length() >= bytes.length : blob.exists()) { // a shorter file is left by an interrupted non-atomic write, so rewrite it
            log.trace("FileSystemBlobStore: setBlob: file exists: {}", blob.getAbsolutePath());
            return; // already exists, so dont overwrite
        }
        // written under a temp name then renamed, so another server never reads a partial blob
        Path tmp = new File(blob.getParentFile(), hash + "." + UUID.randomUUID() + TMP_SUFFIX).toPath();
        try {
            FileUtil.mkdirs(blob.getAbsoluteFile().getParentFile(), setReadable, setReadableOwnerOnly);
            Files.write(tmp, bytes, StandardOpenOption.CREATE_NEW);
            if (setReadable != null || setReadableOwnerOnly != null) {
                tmp.toFile().setReadable(BooleanUtils.toBoolean(setReadable), BooleanUtils.toBoolean(setReadableOwnerOnly));
            }
            Files.move(tmp, blob.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ex2) {
                log.warn("Failed to delete temp blob file: {}", tmp, ex2);
            }
            throw new RuntimeException(blob.getAbsolutePath(), ex);
        }
        log.trace("FileSystemBlobStore: setBlob: wrote file: {} with bytes: {}", blob.getAbsolutePath(), bytes.length);
        if (eventManager != null && enableEvent) {
            try {
                log.info("setBlob: added new blob so tell everyone about it");
                eventManager.fireEvent(new NewFileBlobEvent(hash, blob, root, bytes));
            } catch (ConflictException | BadRequestException | NotAuthorizedException ex) {
                log.error("Exception firing event, but cant do anything about it", ex);
            }
        }
    }

    @Override
    public byte[] getBlob(String hash) {
        File blob = FileSystem2Utils.toFile(root, hash);
        try {
            // no exists() check first: on network storage it is an extra round trip on every read
            byte[] arr = Files.readAllBytes(blob.toPath());
            log.trace("FileSystemBlobStore: getBlob: loaded file: {} for hash: {}", blob.getAbsolutePath(), hash);
            return arr;
        } catch (NoSuchFileException ex) {
            return null;
        } catch (IOException ex) {
            throw new RuntimeException(blob.getAbsolutePath(), ex);
        }
    }

    @Override
    public boolean hasBlob(String hash) {
        File blob = FileSystem2Utils.toFile(root, hash);
        return blob.exists();
    }

    @Override
    public void setReceivingBlobStore(ReceivingBlobStore blobStore) {
        this.receivingBlobStore = blobStore;
        if (fsScanner != null && !fsScanner.isDone()) {
            fsScanner.cancel(true);
        }
        fsScanner = processor.submit(new ScanFileSystem(root.toPath()));
    }

    @Override
    public void pushBlobToQueue(String hash, byte[] bytes) {
        BlobImpl blob = new BlobImpl(hash, bytes);
        if (!queue.contains(blob)) {
            try {
                while (!queue.offer(blob)) {
                    Thread.sleep(10);
                }
            } catch (InterruptedException ex) {
                Logger.getLogger(FileSystem2BlobStore.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void setReadable(boolean setReadable) {
        this.setReadable = setReadable;
    }

    public void setReadableOwnerOnly(boolean setReadableOwnerOnly) {
        this.setReadableOwnerOnly = setReadableOwnerOnly;
    }

    private class ProcessQueue extends TimerTask {

        @Override
        public void run() {
            BlobImpl blob = queue.poll();
            while (blob != null) {
                setBlob(blob.getHash(), blob.getBytes());
                blob = queue.poll();
            }
        }
    }

    private void pushBlobTo(File blob) throws IOException {
        if (this.receivingBlobStore != null && blob.isFile() && blob.exists()) {
            String fileName = blob.getName();
            if (fileName.endsWith(TMP_SUFFIX)) {
                return; // a blob still being written, or left by a failed write
            }
            byte[] arr = FileUtils.readFileToByteArray(blob);
            if (!this.receivingBlobStore.hasBlob(fileName)) {
                this.receivingBlobStore.pushBlobToQueue(fileName, arr);
            }
        }
    }

    private class ScanFileSystem implements Runnable {

        private final Path startPath;

        public ScanFileSystem(final Path startPath) {
            this.startPath = startPath;
        }

        @Override
        public void run() {
            try {
                Files.walkFileTree(this.startPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {

                        if (file != null && attrs != null && !attrs.isDirectory()) {
                            File blob = file.toFile();
                            pushBlobTo(blob);
                        }

                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException ex) {
                log.error("Error walking directory", ex);
            }
        }

    }
}
