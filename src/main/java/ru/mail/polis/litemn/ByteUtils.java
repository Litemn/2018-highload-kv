package ru.mail.polis.litemn;

import java.nio.ByteBuffer;

class ByteUtils {

    private ByteUtils() {
    }

    static long getLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }

    static byte[] getBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }
}
