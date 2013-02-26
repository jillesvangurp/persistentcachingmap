package com.github.jillesvangurp.persistentcachingmap;


public interface PersistentCachingMapCodec<K,V> {
    K deserializeKey(String s);
    String serializeKey(K key);
    long bucketId(K key);
    V deserializeValue(String s);
    String serializeValue(V value);
}
