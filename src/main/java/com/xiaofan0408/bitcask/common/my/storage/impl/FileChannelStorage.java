package com.xiaofan0408.bitcask.common.my.storage.impl;



import com.xiaofan0408.bitcask.common.my.entity.Index;
import com.xiaofan0408.bitcask.common.my.entity.Item;
import com.xiaofan0408.bitcask.common.my.storage.KeyDir;
import com.xiaofan0408.bitcask.common.my.storage.Storage;
import com.xiaofan0408.bitcask.common.my.utils.ByteUtils;
import com.xiaofan0408.bitcask.common.my.utils.Crc32;
import com.xiaofan0408.bitcask.common.my.utils.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author xuzefan  2019/5/10 14:07
 */
public class FileChannelStorage implements Storage {

    // 8 + 8 + 4 + 4
    public static int HEADER_SIZE = 24;

    private final int THRESHOLD = 1024 * 1024 * 1024;

    private final String basePath = "./data";

    private AtomicLong currentActive = new AtomicLong(0);

    private FileChannel writeChannel;

    private FileChannel readChannel;

    private Map<String,FileChannel> readChannelMap = new ConcurrentHashMap<>();

    private AtomicLong writePosition;

    public FileChannelStorage() {
        initCurrentActive();
        initAccessFile();
    }

    @Override
    public void loadData(KeyDir keyDir) {
        try {
            List<File> files = FileUtils.listFile(basePath);
            files.forEach( file -> {
                try {
                    FileChannel channel;
                    if (this.readChannelMap.get(file)!=null){
                        channel = this.readChannelMap.get(file);
                    } else {
                        channel = new RandomAccessFile(file,"r").getChannel();
                    }
                    while (channel.size() != channel.position()) {
                        readOneIndex(channel,file.getPath(),keyDir);
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

    @Override
    public Index ioWrite(long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray) {

        try {
            FileChannel fileChannel = this.getWriteAccessFile();
            fileChannel.position(writePosition.get());
            long start = writePosition.get();
            long length = HEADER_SIZE + key_size + value_size;
            writePosition.addAndGet(length);
            ByteBuffer byteBuffer = getByteBuffer(ts,key_size,value_size,keyArray,valueArray);
            writeFully(fileChannel,byteBuffer);
            String activeFileName = basePath + "/"+ String.format("%s.sst",currentActive);
            return new Index(activeFileName,start,length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Item ioRead(Index info) {
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

    private void initCurrentActive(){
        try {
            List<String> fileNameList = FileUtils.getFile(basePath);
            if (fileNameList.size() == 0) {
                currentActive.set(1);
            } else {
                List<Integer> nameInteger = fileNameList.parallelStream().map( file -> {
                    return Integer.valueOf(file.replace(".sst",""));
                }).collect(Collectors.toList());
                currentActive.set(nameInteger.parallelStream().max(Integer::compare).get());
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
            this.writePosition = new AtomicLong(writeChannel.position());
            this.readChannelMap.put(activeFileName,readChannel);
            FileUtils.listFile(basePath).stream().forEach(f ->{
                try {
                    this.readChannelMap.put(f.getPath(),new RandomAccessFile(f,"r").getChannel());
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            });
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
            if ( this.writePosition.get()> THRESHOLD){
                currentActive.incrementAndGet();
                String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
                File file = new File(activeFileName);
                if (!file.exists()) {
                    file.createNewFile();
                }
                this.writeChannel = new RandomAccessFile(file,"rw").getChannel();
                this.readChannel = new RandomAccessFile(file,"r").getChannel();
                this.writePosition.set(0);
            }
            return this.writeChannel;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private long writeFully(FileChannel ch, ByteBuffer vec) throws IOException {
        return ch.write(vec);
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

    private ByteBuffer getByteBuffer(long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray) throws IOException {
        byte[] data = new byte[HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(data);
        h.putLong(8,ts);
        h.putInt(16,key_size);
        h.putInt(20,value_size);
        Crc32 crc = new Crc32();
        crc.update(data, 8, 16);
        crc.update(keyArray);
        crc.update(valueArray);
        long crc32 = crc.getValue();
        h.putLong(0,crc32);
        byte[] result = ByteUtils.concat(data,keyArray,valueArray);
        return ByteBuffer.wrap(result);
    }

    private void readOneIndex(FileChannel channel,String path,KeyDir keyDir) throws IOException {
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
        String key = new String(keyArray, Charset.forName("utf-8"));
        keyDir.put(key,new Index(path ,start, HEADER_SIZE + key_size + value_size));
        long end = start + HEADER_SIZE + key_size + value_size;
        channel.position(end);
    }

    private Item getItem(long crc32, long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray) {
        String key = new String(keyArray, Charset.forName("utf-8"));
        String value = new String(valueArray, Charset.forName("utf-8"));
        return new Item(crc32, ts, key_size, value_size, key, value);
    }

}
