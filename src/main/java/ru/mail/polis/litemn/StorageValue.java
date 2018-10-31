package ru.mail.polis.litemn;

import java.io.Serializable;

public class StorageValue implements Serializable {

    private final State state;
    private final long time;
    private final byte[] value;

    private StorageValue(State state, long time, byte[] value) {
        this.state = state;
        this.time = time;
        this.value = value;
    }

    static StorageValue exists(byte[] value, long time) {
        return new StorageValue(State.EXISTS, time, value);
    }

    static StorageValue absent() {
        return new StorageValue(State.ABSENT, -1, null);
    }

    public static StorageValue error() {
        return new StorageValue(State.ERROR, -1, null);
    }

    static StorageValue removed(long time) {
        return new StorageValue(State.REMOVED, time, null);
    }

    State getState() {
        return state;
    }

    long getTime() {
        if (state == State.ABSENT) {
            throw new IllegalStateException();
        }
        return time;
    }

    public byte[] getValue() {
        if (state == State.ABSENT || state == State.REMOVED) {
            throw new IllegalStateException();
        }
        return value;
    }

    enum State {
        EXISTS,
        REMOVED,
        ABSENT,
        ERROR
    }
}
