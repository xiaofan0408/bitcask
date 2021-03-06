package com.xiaofan0408.bitcask.v1.common.my.storage;


import com.xiaofan0408.bitcask.v1.common.my.entity.Index;
import com.xiaofan0408.bitcask.v1.common.my.entity.Item;

public interface Storage {

     Item ioRead(Index info);

     Index ioWrite(long ts, int keySize, int valueSize, byte[] keyArray, byte[] valueArray);

    void loadData(KeyDir keyDir);
}
