/*
 * map object
 * Please do not make changes to this file
*/
package io.grpc.filesystem.task2;

import java.io.Serializable;

public class Mapper<K, V> implements Serializable {
    private K word;
    private V value;

    public Mapper(K word, V value) {
        this.word = word;
        this.value = value;
    }

    public K getWord() {
        return word;
    }

    public V getValue() {
        return value;
    }
}

