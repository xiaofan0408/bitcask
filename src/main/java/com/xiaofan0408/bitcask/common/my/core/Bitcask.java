package com.xiaofan0408.bitcask.common.my.core;


import com.xiaofan0408.bitcask.common.my.entity.Item;
import com.xiaofan0408.bitcask.common.my.storage.KeyDir;
import com.xiaofan0408.bitcask.common.my.storage.Storage;
import com.xiaofan0408.bitcask.common.my.storage.impl.FileChannelStorage;
import com.xiaofan0408.bitcask.common.my.utils.StringUtils;

/**
 * @author xuzefan  2019/5/4 17:08
 */
public class Bitcask {

    private KeyDir keyDir;

    public Bitcask(KeyDir keyDir){
        this.keyDir = keyDir;
    }

    public static Builder builder(){
        return new Builder();
    }

    public String get(String key) throws Exception {
        Item item = keyDir.get(key);
        return item.getValue();
    }

    public void put(String key,byte[] values){
        keyDir.put(key,values);
    }

    public void put(String key,String value){
      put(key, StringUtils.getStringByte(value));
    }

    public void putAsync(String key,byte[] values) {
        keyDir.putAsync(key,values);
    }

    public static class Builder{

        private Storage storage;

        public Builder fileChannelStorage(){
            this.storage = new FileChannelStorage();
            return this;
        }

        public Builder storage(Storage storage){
            this.storage = storage;
            return this;
        }

        public Bitcask build(){
            if (this.storage == null) {
                this.storage = new FileChannelStorage();
            }
            KeyDir keyDir = new KeyDir(storage);
            return new Bitcask(keyDir);
        }

    }

}
