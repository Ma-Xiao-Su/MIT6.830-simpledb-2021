package simpledb.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class IteratorWrapper<T> implements Iterator<T> {
    private int cursor;

    private T[] objects;

    public IteratorWrapper(final T[] objects) {
        this.cursor = 0;
        this.objects = objects;
    }

    @Override
    public boolean hasNext() {
        return cursor < objects.length && objects[cursor] != null;
    }

    @Override
    public T next() {
        if (cursor >= objects.length) {
            throw new NoSuchElementException();
        }
        T value = objects[cursor];
        ++cursor;
        return value;
    }
}