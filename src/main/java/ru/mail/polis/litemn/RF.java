package ru.mail.polis.litemn;

import org.jetbrains.annotations.NotNull;

class RF {
    private final int ack;
    private final int from;

    private RF(int ack, int from) {
        if (from <= 0) {
            throw new IllegalArgumentException("From must be positive");
        }
        if (ack <= 0 || from < ack) {
            throw new IllegalArgumentException("ack must be positive and less or eq from");
        }
        this.ack = ack;
        this.from = from;
    }

    static RF quorum(int count) {
        return new RF(count / 2 + 1, count);
    }

    static RF from(@NotNull String replicas) {
        String[] split = replicas.split("/");
        if (split.length != 2) {
            throw new IllegalArgumentException("Wrong replicas format");
        }
        return new RF(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
    }

    int getAck() {
        return ack;
    }

    int getFrom() {
        return from;
    }
}
