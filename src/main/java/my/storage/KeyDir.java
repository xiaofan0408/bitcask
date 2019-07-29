package my.storage;

import my.entity.Index;
import my.entity.Item;
import my.utils.CrcUtil;
import my.utils.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xuzefan  2019/5/10 14:08
 */
public class KeyDir {

    private Map<String, Index> TABLE = new ConcurrentHashMap<>();

    private Storage storage;

    public KeyDir(Storage storage) {
        this.storage = storage;
        this.reloadIndex();
    }

    public Item get(String key) throws Exception{
        Index info = TABLE.get(key);
        if (info == null){
            throw new Exception("not found.");
        }
        Item item = storage.ioRead(info);
        return item;
    }

    public void put(String key,String value){
        byte[] keyArray = StringUtils.getStringByte(key);
        byte[] valueArray = StringUtils.getStringByte(value);
        int key_size = keyArray.length;
        int value_size = valueArray.length;
        long ts = System.currentTimeMillis();
        String crcStr = String.format("%d%d%d%s%s",ts , key_size , value_size , key ,value);
        long crc32 = CrcUtil.getCrc32(crcStr);
        Index info = storage.ioWrite(crc32, ts, key_size, value_size, keyArray, valueArray);
        this.TABLE.put(key,info);
    }

    public void put(String key,Index index){
        this.TABLE.put(key,index);
    }

    public void reloadIndex() {
        this.storage.loadData(this);
    }

}
