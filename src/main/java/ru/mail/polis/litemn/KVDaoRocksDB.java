package ru.mail.polis.litemn;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * RocksDB KV dao impl
 */
public class KVDaoRocksDB implements KVDao {

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
            if (bytes == null) {
                throw new NoSuchElementException();
            }
            return bytes;
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        db.close();
    }
}
