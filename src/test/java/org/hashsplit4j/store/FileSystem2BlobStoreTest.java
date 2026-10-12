package org.hashsplit4j.store;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;
import org.hashsplit4j.utils.FileSystem2Utils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileSystem2BlobStoreTest {

    private static final String HASH = "0123456789abcdef0123456789abcdef01234567";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSetBlobLeavesNoTempFile() throws Exception {
        FileSystem2BlobStore store = new FileSystem2BlobStore(folder.getRoot());
        store.setBlob(HASH, "hello".getBytes());

        Assert.assertEquals("hello", new String(store.getBlob(HASH)));
        try (Stream<Path> files = Files.walk(folder.getRoot().toPath())) {
            Assert.assertEquals(0, files.filter(p -> p.toString().endsWith(".tmp")).count());
        }
    }

    @Test
    public void testSetBlobRepairsTruncatedFile() throws Exception {
        File blob = FileSystem2Utils.toFile(folder.getRoot(), HASH);
        blob.getParentFile().mkdirs();
        Files.write(blob.toPath(), "hel".getBytes());

        FileSystem2BlobStore store = new FileSystem2BlobStore(folder.getRoot());
        store.setBlob(HASH, "hello".getBytes());

        Assert.assertEquals("hello", new String(store.getBlob(HASH)));
    }

    @Test
    public void testSetBlobKeepsExistingEmptyBlob() throws Exception {
        FileSystem2BlobStore store = new FileSystem2BlobStore(folder.getRoot());
        store.setBlob(HASH, new byte[0]);
        Path blob = FileSystem2Utils.toFile(folder.getRoot(), HASH).toPath();
        Object fileKey = Files.readAttributes(blob, BasicFileAttributes.class).fileKey();

        store.setBlob(HASH, new byte[0]);

        Assert.assertEquals(fileKey, Files.readAttributes(blob, BasicFileAttributes.class).fileKey());
        Assert.assertEquals(0, store.getBlob(HASH).length);
    }

    @Test
    public void testSetBlobKeepsLongerExistingBlob() throws Exception {
        FileSystem2BlobStore store = new FileSystem2BlobStore(folder.getRoot());
        store.setBlob(HASH, "hello".getBytes());
        store.setBlob(HASH, "hi".getBytes());

        Assert.assertEquals("hello", new String(store.getBlob(HASH)));
    }

    @Test
    public void testSetBlobKeepsExistingBlob() throws Exception {
        FileSystem2BlobStore store = new FileSystem2BlobStore(folder.getRoot());
        store.setBlob(HASH, "hello".getBytes());
        store.setBlob(HASH, "other".getBytes());

        Assert.assertEquals("hello", new String(store.getBlob(HASH)));
    }
}
