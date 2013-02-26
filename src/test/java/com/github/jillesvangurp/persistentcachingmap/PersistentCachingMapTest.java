package com.github.jillesvangurp.persistentcachingmap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.jillesvangurp.iterables.Iterables;

@Test
public class PersistentCachingMapTest {

    private Path dataDir;

    @BeforeMethod
    public void before() throws IOException {
        dataDir = Files.createTempDirectory("kv");
    }

    @AfterMethod
    public void afterEach() throws IOException {
        FileUtils.deleteDirectory(dataDir.toFile());
    }

    private final class StringCodec implements PersistentCachingMapCodec<Long,String> {

        @Override
        public Long deserializeKey(String s) {
            return Long.valueOf(s);
        }

        @Override
        public String serializeKey(Long key) {
            return "" + key;
        }

        @Override
        public long bucketId(Long key) {
            return key/200;
        }

        @Override
        public String deserializeValue(String s) {
            return s;
        }

        @Override
        public String serializeValue(String value) {
            return value;
        }
    }

    private String dir() {
        return dataDir.toFile().getAbsolutePath();
    }

    public void shouldCalculateCorrectBucketPath() throws IOException {
        try (PersistentCachingMap<Long,String> kv = new PersistentCachingMap<>(dir(), new StringCodec(), 200)) {
            assertThat(kv.bucketPath(1234).getPath(), is(dir()+"/000/000/0000001234"));
            assertThat(kv.bucketPath(123456789).getPath(), is(dir()+"/012/345/0123456789"));
        }
    }

    public void shouldPersistValues() throws IOException {
        try (PersistentCachingMap<Long,String> kv = new PersistentCachingMap<>(dir(), new StringCodec(), 200)) {
            kv.put(1l, "1");
            kv.put(2l, "2");
            kv.put(1000l, "1000");
        }
        // by now the kv should be properly flushed and closed; lets reopen it and assert that everything is there
        try (PersistentCachingMap<Long,String> kv = new PersistentCachingMap<>(dir(), new StringCodec(), 200)) {
            assertThat(kv.get(1l), is("1"));
            assertThat(kv.get(2l), is("2"));
            assertThat(kv.get(1000l), is("1000"));
        }
    }

    public void shouldIterateEntries() throws IOException {
        try (PersistentCachingMap<Long,String> kv = new PersistentCachingMap<>(dir(), new StringCodec(), 200)) {
            kv.put(1l, "1");
            kv.put(2l, "2");
            kv.put(1000l, "1000");
            kv.put(100000l, "100000");
            assertThat(Iterables.count(kv), is(4l));
        }
    }
}
