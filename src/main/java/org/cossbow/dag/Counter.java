package org.cossbow.dag;

public class Counter {
    private long value;

    public Counter() {
    }

    public Counter(long value) {
        this.value = value;
    }

    public long get() {
        return value;
    }

    public long incAndGet() {
        return ++value;
    }

    public long decAndGet() {
        return --value;
    }

    @Override
    public String toString() {
        return "Counter(" + value + ')';
    }
}
