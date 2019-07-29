package my.core;

import my.entity.Item;
import my.storage.KeyDir;
import my.storage.Storage;


/**
 * @author xuzefan  2019/5/4 17:08
 */
public class Bitcask {

    private KeyDir keyDir;

    private Storage storage;

    public Bitcask(){
        storage = new Storage();
        keyDir = new KeyDir(storage);
    }

    public String get(String key) throws Exception {
        Item item = keyDir.get(key);
        return item.getValue();
    }

    public void put(String key,String value){
       keyDir.put(key,value);
    }

}
