package my.core;

import my.utils.CrcUtil;
import my.utils.FileUtils;


import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author xuzefan  2019/5/4 17:08
 */
public class Bitcask {

    private Map<String, Item> TABLE = new ConcurrentHashMap<>();

    private final int THRESHOLD = 10000;

    private final String basePath = "./data";

    private int currentActive = 0;

    public Bitcask(){
        initData();
    }

    private void initData(){
        initCurrentActive();
    }

    private void initCurrentActive(){
        try {
            List<String> fileNameList = FileUtils.getFile(basePath);
            if (fileNameList.size() == 0) {
                currentActive = 1;
            } else {
                List<Integer> nameInteger = fileNameList.parallelStream().map( file -> {
                    return Integer.valueOf(file.replace(".sst",""));
                }).collect(Collectors.toList());
                currentActive = nameInteger.parallelStream().max(Integer::compare).get();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private Item ioWrite(long crc32, long ts, int key_size, int value_size, String key, String value) {
        return null;
    }


    public String get(String key) throws Exception {
        Item info = TABLE.get(key);
        if (info == null){
            throw new Exception("not found.");
        }

        pickled_data = io_read(*info)
        data = pickle.loads(pickled_data)
        return data[5]
    }

    public void put(String key,String value){
        int key_size = key.length();
        int value_size = value.length();
        long ts = System.currentTimeMillis();
        String crcStr = String.format("%d%d%d%s%s",ts , key_size , value_size , key ,value);
        long crc32 = CrcUtil.getCrc32(crcStr);
        Item info = ioWrite(crc32, ts, key_size, value_size, key, value);
        this.TABLE.put(key,info);
    }


}
