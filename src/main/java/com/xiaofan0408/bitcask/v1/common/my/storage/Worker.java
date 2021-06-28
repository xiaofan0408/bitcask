package com.xiaofan0408.bitcask.v1.common.my.storage;



import com.xiaofan0408.bitcask.v1.common.my.entity.Index;

import java.util.concurrent.BlockingQueue;

public class Worker  extends Thread {

    private BlockingQueue<ItemEvent> blockingQueue;

    private Storage storage;

    private KeyDir keyDir;

    public Worker(BlockingQueue<ItemEvent> blockingQueue,Storage storage,KeyDir keyDir){
        this.blockingQueue = blockingQueue;
        this.keyDir = keyDir;
        this.storage = storage;
    }

    @Override
    public void run() {

        // loop while the parent is started
        while (true) {
            try {
                ItemEvent e = blockingQueue.take();
                Index info = storage.ioWrite(e.getTs(), e.getKeySize(), e.getValueSize(), e.getKeyArray(), e.getValueArray());
                keyDir.put(e.getKey(),info);
            } catch (InterruptedException ie) {
                break;
            }
        }
    }
}
