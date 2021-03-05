package com.xiaofan0408.bitcask.common.my.storage;



import com.xiaofan0408.bitcask.common.my.entity.Index;
import com.xiaofan0408.bitcask.common.my.entity.Item;
import com.xiaofan0408.bitcask.common.my.utils.StringUtils;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author xuzefan  2019/5/10 14:08
 */
public class KeyDir {

    private Map<String, Index> TABLE = new ConcurrentHashMap<>();

    private Storage storage;

    private BlockingQueue<ItemEvent> blockingQueue;

    private int DEFAULT_QUEUE_SIZE= 1024;

    private Worker worker;

    public KeyDir(Storage storage) {
        this.storage = storage;
        this.reloadIndex();
        this.blockingQueue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);
        worker = new Worker(blockingQueue,storage,this);
        worker.start();
    }

    public Item get(String key) throws Exception{
        Index info = TABLE.get(key);
        if (info == null){
            throw new Exception("not found.");
        }
        Item item = storage.ioRead(info);
        return item;
    }

    public void put(String key,byte[] values){
        byte[] keyArray = StringUtils.getStringByte(key);
        byte[] valueArray = values;
        int key_size = keyArray.length;
        int value_size = valueArray.length;
        long ts = System.currentTimeMillis();
        Index info = storage.ioWrite(ts, key_size, value_size, keyArray, valueArray);
        this.TABLE.put(key,info);
    }

    public void put(String key,Index index){
        this.TABLE.put(key,index);
    }

    public void reloadIndex() {
        this.storage.loadData(this);
    }

    public void putAsync(String key, byte[] values) {
        byte[] keyArray = StringUtils.getStringByte(key);
        byte[] valueArray = values;
        int key_size = keyArray.length;
        int value_size = valueArray.length;
        long ts = System.currentTimeMillis();
        ItemEvent itemEvent = new ItemEvent();
        itemEvent.setKey(key);
        itemEvent.setKeyArray(keyArray);
        itemEvent.setValueArray(valueArray);
        itemEvent.setKeySize(key_size);
        itemEvent.setValueSize(value_size);
        itemEvent.setTs(ts);

        boolean interrupted = false;
        try {
            while (true) {
                try {
                    blockingQueue.put(itemEvent);
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
