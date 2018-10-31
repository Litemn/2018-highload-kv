package ru.mail.polis.litemn;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * RocksDB KV dao impl
 */
public class KVDaoRocksDB implements KVDao {

    private static final byte[] EXISTS = new byte[]{1};
    private static final byte[] REMOVED = new byte[]{0};
    private static final byte[] EMPTY = new byte[]{};

    static {
        //load native library
        RocksDB.loadLibrary();
    }

    private final RocksDB db;

    public KVDaoRocksDB(File dir) {
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("File must be dir");
        }
        try (final Options options = new Options().setCreateIfMissing(true)) {
            db = RocksDB.open(options, dir.getPath());
        } catch (RocksDBException e) {
            throw new IllegalStateException("Fail to init rocksDB", e);
        }
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        try {
            byte[] bytes = db.get(key);
            if (bytes == null || bytes.length == 0 || bytes[0] == REMOVED[0]) {
                throw new NoSuchElementException();
            }
            return Arrays.copyOfRange(bytes, EXISTS.length + Long.BYTES, bytes.length);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        try {
            db.put(key, getStoredValue(value, EXISTS));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        try {
            byte[] removed = getStoredValue(EMPTY, REMOVED);
            db.put(key, removed);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public boolean removeInternal(@NotNull byte[] key) {
        try {
            byte[] removed = getStoredValue(EMPTY, REMOVED);
            db.put(key, removed);
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    public StorageValue getInternal(@NotNull byte[] key) throws IOException {
        byte[] bytes;
        try {
            bytes = db.get(key);
            if (bytes == null || bytes.length == 0) {
                return StorageValue.absent();
            }
            long time = ByteUtils.getLong(Arrays.copyOfRange(bytes, EXISTS.length, Long.BYTES + 1));
            if (bytes[0] == REMOVED[0]) {
                return StorageValue.removed(time);
            }
            byte[] value = Arrays.copyOfRange(bytes, EXISTS.length + Long.BYTES, bytes.length);
            return StorageValue.exists(value, time);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }

    }

    public boolean upsertInternal(@NotNull byte[] key, @NotNull byte[] value) {
        try {
            db.put(key, getStoredValue(value, EXISTS));
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    /**
     * Store data in such format
     * +---------+-----------+------------+
     * |exist flag | timestamp| payload   |
     * +---------+-----------+------------+
     *
     * @param value value for store
     * @param flag  flag {@code ru.mail.polis.litemn.KVDaoRocksDB#EXISTS} {@code ru.mail.polis.litemn.KVDaoRocksDB#REMOVED}
     * @return array of bytes in correct form
     */
    private byte[] getStoredValue(@NotNull byte[] value, @NotNull byte[] flag) {
        byte[] stored = new byte[value.length + 1 + Long.BYTES];
        byte[] time = ByteUtils.getBytes(System.currentTimeMillis());
        System.arraycopy(flag, 0, stored, 0, flag.length);
        System.arraycopy(time, 0, stored, flag.length, time.length);
        System.arraycopy(value, 0, stored, flag.length + time.length, value.length);
        return stored;
    }

    @Override
    public void close() {
        db.close();
    }
}
