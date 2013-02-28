/**
 * Copyright (c) 2013, Jilles van Gurp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.github.jillesvangurp.persistentcachingmap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.jillesvangurp.iterables.Iterables;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.ProcessingIterable;
import com.jillesvangurp.iterables.Processor;

/**
 * <p>
 * Persistent caching map for working with large map like data structures that do not fit into memory. Like any map,
 * this map uses buckets to store things. Each bucket is persisted in a file and has an id that is a long. To persist
 * and calculate the bucket ids from the key, a {@link PersistentCachingMapCodec} instance is used. An instance of this
 * codec must be provided at construction time.
 * </p>
 *
 * <p>
 * This is not a proper database and it does not make guarantees with respect to the traditional ACID properties. In
 * particular, changes are not written right away, which means you risk data loss in the case of e.g. a crash.
 * </p>
 *
 * <p>
 * Some of the map operations throw UnsupportedOperations because the operation would be too expensive. So, don't use
 * anything that returns a set, size, and containsValue.
 * </p>
 *
 * <p>
 * To support iterating all entries, this class implements Iterable.
 * </p>
 *
 * @param <Key>
 * @param <Value>
 */
public class PersistentCachingMap<Key, Value> implements Iterable<Map.Entry<Key, Value>>, Map<Key, Value>, Closeable {
    private final PersistentCachingMapCodec<Key, Value> codec;
    private final LoadingCache<Long, Bucket> cache;
    private final Set<Long> bucketIds = new ConcurrentSkipListSet<>();
    private final IdLock idLock = new IdLock();

    private final String dataDir;

    public PersistentCachingMap(String dataDir, PersistentCachingMapCodec<Key, Value> codec, long cacheSize) {
        this.codec = codec;
        this.dataDir = dataDir;
        CacheLoader<Long, Bucket> loader = new CacheLoader<Long, Bucket>() {
            @Override
            public Bucket load(Long id) throws Exception {
                Bucket bucket = new Bucket(id);
                idLock.acquire(id);
                try {
                    bucket.read();
                } finally {
                    idLock.release(id);
                }
                return bucket;
            }
        };
        cache = CacheBuilder.newBuilder().maximumSize(cacheSize).removalListener(new RemovalListener<Long, Bucket>() {
            @Override
            public void onRemoval(RemovalNotification<Long, Bucket> notification) {
                Long bucketId = notification.getKey();
                // make sure changed buckets are written on eviction
                idLock.acquire(bucketId);
                try {
                    notification.getValue().write();
                } finally {
                    idLock.release(bucketId);
                }
            }
        }).build(loader);
        readBucketIds();
    }

    @SuppressWarnings("unchecked")
    // Map.get is unchecked
    @Override
    public Value get(Object key) {
        try {
            long bucketId = codec.bucketId((Key) key);
            if (bucketIds.contains(bucketId)) {
                Value value = cache.get(bucketId).get((Key) key);
                // return a clone,  this ensures people don't accidentally modify objects in the map.
                return codec.deserializeValue(codec.serializeValue(value));
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }

    @Override
    public Value put(Key key, Value value) {
        try {
            long bucketId = codec.bucketId(key);
            synchronized (this) {
                if (!bucketIds.contains(bucketId)) {
                    bucketIds.add(bucketId);
                }
                Bucket bucket = cache.get(bucketId);
                bucket.put(key, value);
            }
            return value;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    // Map.remove is unchecked
    @Override
    public Value remove(Object key) {
        try {
            long bucketId = codec.bucketId((Key) key);
            synchronized (this) {
                if (!bucketIds.contains(bucketId)) {
                    bucketIds.add(bucketId);
                }
                Bucket bucket = cache.get(bucketId);
                return bucket.remove((Key) key);
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void clear() {
        for (Long bucketId : bucketIds) {
            try {
                cache.get(bucketId).clear();
            } catch (ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    void writeBucketIds() {
        if (bucketIds.size() > 0) {
            File path = new File(dataDir, "bucketIds.gz");
            File dir = path.getParentFile();
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    throw new IllegalStateException("could not create directory " + dir);
                }
            }

            try (BufferedWriter out = gzipFileWriter(path)) {
                for (Long id : bucketIds) {
                    out.write("" + id + "\n");
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    void readBucketIds() {
        File bucketIdFile = new File(dataDir, "bucketIds.gz");
        if (bucketIdFile.exists()) {
            try (LineIterable iterable = new LineIterable(gzipFileReader(bucketIdFile))) {
                for (String line : iterable) {
                    bucketIds.add(Long.valueOf(line.trim()));
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public void flush() {
        cache.invalidateAll();
        writeBucketIds();
    }

    File bucketPath(long id) {
        String paddedId = StringUtils.leftPad("" + id, 10, '0');
        String d1 = paddedId.substring(0, 3);
        String d2 = paddedId.substring(3, 6);

        return new File(dataDir, d1 + "/" + d2 + "/" + paddedId);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("This would be expensive and Map.size returns an int instead of a long.");
    }

    @Override
    public boolean isEmpty() {
        return bucketIds.size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("This would be expensive");
    }

    @Override
    public void putAll(Map<? extends Key, ? extends Value> m) {
        for (Entry<? extends Key, ? extends Value> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }

    }

    @Override
    public Set<Key> keySet() {
        throw new UnsupportedOperationException("This would be expensive");
    }

    @Override
    public Collection<Value> values() {
        throw new UnsupportedOperationException("This would be expensive");
    }

    @Override
    public Set<java.util.Map.Entry<Key, Value>> entrySet() {
        throw new UnsupportedOperationException("This would be expensive");
    }

    @Override
    public Iterator<java.util.Map.Entry<Key, Value>> iterator() {
        Processor<Long, Iterable<java.util.Map.Entry<Key, Value>>> processor = new Processor<Long, Iterable<Entry<Key, Value>>>() {

            @Override
            public Iterable<java.util.Map.Entry<Key, Value>> process(Long bucketId) {
                try {
                    return cache.get(bucketId);
                } catch (ExecutionException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
        ProcessingIterable<Long, Iterable<Map.Entry<Key, Value>>> iterables = new ProcessingIterable<Long, Iterable<Map.Entry<Key, Value>>>(
                bucketIds.iterator(), processor);
        return Iterables.compose(iterables).iterator();
    }

    // private static BufferedWriter bzipFileWriter(File file) throws IOException {
    // return new BufferedWriter(new OutputStreamWriter(new BZip2CompressorOutputStream(new FileOutputStream(file)),
    // Charset.forName("utf-8")),64*1024);
    // }
    //
    // private static BufferedReader bzipFileReader(File file) throws IOException {
    // return new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(new
    // FileInputStream(file)),Charset.forName("utf-8")));
    // }
    private static BufferedWriter gzipFileWriter(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), Charset.forName("utf-8")), 64 * 1024);
    }

    private static BufferedReader gzipFileReader(File file) throws IOException {
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), Charset.forName("utf-8")));
    }

    class IdLock {
        private final Set<Long> ids = new ConcurrentSkipListSet<>();
        Lock bucketLock = new ReentrantLock();

        public void acquire(Long id) {
            while(true) {
                try {
                    bucketLock.lock();
                    if(!ids.contains(id)) {
                        ids.add(id);
                        return;
                    }
                } finally {
                    bucketLock.unlock();
                }
            }
        }

        public void release(Long id) {
            try {
                bucketLock.lock();
                ids.remove(id);
            } finally {
                bucketLock.unlock();
            }

        }
    }

    public class Bucket implements Iterable<Map.Entry<Key, Value>> {
        long id;
        AtomicBoolean changed = new AtomicBoolean(false);

        Map<Key, Value> map = new ConcurrentHashMap<>();

        public Bucket(long id) {
            this.id = id;
        }

        public void clear() {
            map.clear();
        }

        public Value get(Key key) {
            return map.get(key);
        }

        public void put(Key key, Value value) {
            map.put(key, value);
            changed.set(true);
        }

        public Value remove(Key key) {
            changed.set(true);
            return map.remove(key);
        }

        public void write() {
            if (changed.get()) {
                // prevent somebody reading a partially written bucket by synchronizing on the cache
                // need more clever synchronization
                File path = bucketPath(id);
                if (map.isEmpty() && path.exists()) {
                    // map is empty, remove the file if it existed and delete the bucket from the bucketIds
                    if (!path.delete()) {
                        throw new IllegalStateException("could not delete " + path);
                    }
                    bucketIds.remove(id);
                } else {
                    File dir = path.getParentFile();
                    if (!dir.exists()) {
                        if (!dir.mkdirs()) {
                            throw new IllegalStateException("could not create directory " + dir);
                        }
                    }
                    try (BufferedWriter out = gzipFileWriter(path)) {
                        for (Entry<Key, Value> e : map.entrySet()) {
                            out.write("" + codec.serializeKey(e.getKey()) + ";" + codec.serializeValue(e.getValue()) + "\n");
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException("could not write file " + path, e);
                    }
                }
            }
        }

        public void read() {
            File path = bucketPath(id);
            if (path.exists()) {
                try (LineIterable iterable = new LineIterable(gzipFileReader(path))) {
                    for (String line : iterable) {
                        int idx = line.indexOf(';');
                        Key key = codec.deserializeKey(line.substring(0, idx));
                        map.put(key, codec.deserializeValue(line.substring(idx + 1)));
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("could not read file " + path, e);
                }
            }
        }

        public long size() {
            return map.size();
        }

        @Override
        public Iterator<java.util.Map.Entry<Key, Value>> iterator() {
            return map.entrySet().iterator();
        }
    }
}
