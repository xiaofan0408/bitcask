package com.xiaofan0408.bitcask.v2.storage;




public interface Storage {

    Entry read(long offset);

    void write(Entry entry);

    void close(boolean sync);

    void sync();
}

