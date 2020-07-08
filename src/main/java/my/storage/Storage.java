package my.storage;

import my.entity.Index;
import my.entity.Item;

public interface Storage {

     Item ioRead(Index info);

     Index ioWrite(long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray);

    void loadData(KeyDir keyDir);
}
