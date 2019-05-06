package my.core;

import my.utils.CrcUtil;
import my.utils.FileUtils;
import my.utils.SerializationUtil;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author xuzefan  2019/5/4 17:08
 */
public class Bitcask {

    private Map<String, Index> TABLE = new ConcurrentHashMap<>();

    private final int THRESHOLD = 1024 * 1024;

    private final String basePath = "./data";

    private int currentActive = 0;

    private RandomAccessFile readAccessFile;

    private RandomAccessFile writeAccessFile;

    public Bitcask(){
        initData();
    }

    private void initData(){
        initCurrentActive();
        initAccessFile();
        loadData();
    }

    private void loadData() {
        try {
            List<File> files = FileUtils.listFile(basePath);
            files.forEach( file -> {

            });
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    private void initAccessFile(){
        String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
        File file = new File(activeFileName);
        try{
            if (!file.exists()) {
                file.createNewFile();
            }
            this.readAccessFile = new RandomAccessFile(file,"r");
            this.writeAccessFile = new RandomAccessFile(file,"rw");
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private RandomAccessFile getReadAccessFile(Index info){
        String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
        if (info.getActiveFileName().equals(activeFileName)) {
            return this.readAccessFile;
        } else {
            File file = new File(activeFileName);
            try {
                return  new RandomAccessFile(file,"r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private RandomAccessFile getWriteAccessFile(){
        try {
            if ( this.writeAccessFile.length() > THRESHOLD){
                currentActive += 1;
                String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
                File file = new File(activeFileName);
                if (!file.exists()) {
                    file.createNewFile();
                }
                this.writeAccessFile = new RandomAccessFile(file,"rw");
            }
            return this.writeAccessFile;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Index ioWrite(long crc32, long ts, int key_size, int value_size, String key, String value) {

        try {
            RandomAccessFile randomAccessFile = this.getWriteAccessFile();
            Item item = getItem(crc32,ts,key_size,value_size,key,value);
            byte[] byteArray = SerializationUtil.serialize(item);
            randomAccessFile.seek(randomAccessFile.length());
            long start = randomAccessFile.getFilePointer();
            long length = byteArray.length;
            randomAccessFile.write(byteArray);
            String activeFileName = basePath + String.format("%s.sst",currentActive);
            return new Index(activeFileName,start,length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Item getItem(long crc32, long ts, int key_size, int value_size, String key, String value){
        return new Item(crc32,ts,key_size,value_size,key,value);
    }

    private byte[] ioRead(Index info) {
        RandomAccessFile randomAccessFile = this.getReadAccessFile(info);
        try {
            byte[] byteArray = new byte[(int)info.getLength()];
            randomAccessFile.seek(info.getStart());
            randomAccessFile.read(byteArray,0,(int)info.getLength());
            return Arrays.copyOf(byteArray,(int)info.getLength());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String get(String key) throws Exception {
        Index info = TABLE.get(key);
        if (info == null){
            throw new Exception("not found.");
        }
        byte[] pickled_data = ioRead(info);
        Item item = (Item) SerializationUtil.deserialize(pickled_data);
        return item.getValue();
    }

    public void put(String key,String value){
        int key_size = key.length();
        int value_size = value.length();
        long ts = System.currentTimeMillis();
        String crcStr = String.format("%d%d%d%s%s",ts , key_size , value_size , key ,value);
        long crc32 = CrcUtil.getCrc32(crcStr);
        Index info = ioWrite(crc32, ts, key_size, value_size, key, value);
        this.TABLE.put(key,info);
    }

}
