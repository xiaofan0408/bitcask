package my.core;

import my.utils.CrcUtil;
import my.utils.FileUtils;
import my.utils.SerializationUtil;
import my.utils.StringUtils;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author xuzefan  2019/5/4 17:08
 */
public class Bitcask {

    // 8 + 8 + 4 + 4
    private static int HEADER_SIZE = 24;

    private Map<String, Index> TABLE = new ConcurrentHashMap<>();

    private final int THRESHOLD = 1024 * 1024;

    private final String basePath = "./data";

    private int currentActive = 0;

    private FileChannel writeChannel;

    private FileChannel readChannel;

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
                try {
                    FileChannel channel = new RandomAccessFile(file,"r").getChannel();
                    while (channel.size() != channel.position()) {
                        readOneIndex(channel,file.getPath());
                    }
                    channel.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void readOneIndex(FileChannel channel,String path) throws IOException {
        long start = channel.position();
        byte[] bytes = new byte[HEADER_SIZE];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        channel.read(byteBuffer);
        byteBuffer.rewind();
        long crc32 = byteBuffer.getLong();
        long ts = byteBuffer.getLong();
        int key_size = byteBuffer.getInt();
        int value_size = byteBuffer.getInt();
        byte[] keyArray = new byte[key_size];
        ByteBuffer k = ByteBuffer.wrap(keyArray);
        channel.read(k);
        String key = new String(keyArray,Charset.forName("utf-8"));
        this.TABLE.put(key,new Index(path ,start, HEADER_SIZE + key_size + value_size));
        long end = start + HEADER_SIZE + key_size + value_size;
        channel.position(end);
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
            this.readChannel = new RandomAccessFile(file,"r").getChannel();
            this.writeChannel = new RandomAccessFile(file,"rw").getChannel();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private FileChannel getReadAccessFile(Index info){
        String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
        if (info.getActiveFileName().equals(activeFileName)) {
            return this.readChannel;
        } else {
            File file = new File(info.getActiveFileName());
            try {
                return  new RandomAccessFile(file,"r").getChannel();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private FileChannel getWriteAccessFile(){
        try {
            if ( this.writeChannel.size() > THRESHOLD){
                currentActive += 1;
                String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
                File file = new File(activeFileName);
                if (!file.exists()) {
                    file.createNewFile();
                }
                this.writeChannel = new RandomAccessFile(file,"rw").getChannel();
                this.readChannel = new RandomAccessFile(file,"r").getChannel();
            }
            return this.writeChannel;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Index ioWrite(long crc32, long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray) {

        try {
            FileChannel fileChannel = this.getWriteAccessFile();
            fileChannel.position(fileChannel.size());
            long start = fileChannel.position();
            long length = HEADER_SIZE + key_size + value_size;
            ByteBuffer[] byteBuffers = getByteBuffer(crc32,ts,key_size,value_size,keyArray,valueArray);
            writeFully(fileChannel,byteBuffers);
            String activeFileName = basePath + "/"+ String.format("%s.sst",currentActive);
            return new Index(activeFileName,start,length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private long writeFully(FileChannel ch, ByteBuffer[] vec) throws IOException {
        synchronized (ch) {
            long len = length(vec);
            long w = 0;
            while (w < len) {
                long ww = ch.write(vec);
                if (ww > 0) {
                    w += ww;
                } else if (ww == 0) {
                    Thread.yield();
                } else {
                    return w;
                }
            }
            return w;
        }
    }

    private long length(ByteBuffer[] vec) {
        long length = 0;
        for (int i = 0; i < vec.length; i++) {
            length += vec[i].remaining();
        }
        return length;
    }


    private Item getItem(long crc32, long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray){
        String key = new String(keyArray,Charset.forName("utf-8"));
        String value = new String(valueArray,Charset.forName("utf-8"));
        return new Item(crc32,ts,key_size,value_size,key,value);
    }

    private Item ioRead(Index info) {
        FileChannel fileChannel = this.getReadAccessFile(info);
        try {
            byte[] byteArray = new byte[(int)info.getLength()];
            ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
            fileChannel.position(info.getStart());
            fileChannel.read(byteBuffer);
            return byteBufferToItem(byteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private Item byteBufferToItem(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        long crc32 = byteBuffer.getLong();
        long ts = byteBuffer.getLong();
        int key_size = byteBuffer.getInt();
        int value_size = byteBuffer.getInt();
        byte[] keyArray = new byte[key_size];
        byte[] valueArray = new byte[value_size];
        byteBuffer.get(keyArray);
        byteBuffer.get(valueArray);
        return getItem(crc32,ts,key_size,value_size,keyArray,valueArray);
    }

    private ByteBuffer[] getByteBuffer(long crc32, long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray){
        byte[] data = new byte[HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(data);
        h.putLong(0 ,crc32);
        h.putLong(8,ts);
        h.putInt(16,key_size);
        h.putInt(20,value_size);
        ByteBuffer key = ByteBuffer.wrap(keyArray);
        ByteBuffer value = ByteBuffer.wrap(valueArray);
        return new ByteBuffer[]{h,key,value};
    }

    public String get(String key) throws Exception {
        Index info = TABLE.get(key);
        if (info == null){
            throw new Exception("not found.");
        }
        Item item = ioRead(info);
        return item.getValue();
    }

    public void put(String key,String value){
        byte[] keyArray = StringUtils.getStringByte(key);
        byte[] valueArray = StringUtils.getStringByte(value);
        int key_size = keyArray.length;
        int value_size = valueArray.length;
        long ts = System.currentTimeMillis();
        String crcStr = String.format("%d%d%d%s%s",ts , key_size , value_size , key ,value);
        long crc32 = CrcUtil.getCrc32(crcStr);
        Index info = ioWrite(crc32, ts, key_size, value_size, keyArray, valueArray);
        this.TABLE.put(key,info);
    }

}
